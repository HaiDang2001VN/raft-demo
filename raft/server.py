import grpc
import asyncio
import random
import sys
import time
from datetime import datetime, timedelta
from concurrent import futures
from enum import Enum
from raft_pb2 import *
from raft_pb2_grpc import RaftNodeServicer, add_RaftNodeServicer_to_server, RaftNodeStub
from config import PEER_PORTS, HB_PERIOD, MAX_RETRIES, DIS_TIMEOUT, COMMIT_PERIOD

class RaftNode(RaftNodeServicer):
    def __init__(self, node_id):
        self.node_id = node_id
        self.peers = PEER_PORTS
        self.disconnected = set()
        self.current_term = 0
        self.voted_for = None
        self.log = []
        self.commit_index = 0
        self.last_applied = 0
        self.state = 'follower'
        self.leader_id = None
        self.next_index = {}
        self.match_index = {}
        self.kv_store = {}
        self.lock = asyncio.Lock()
        
        # Election and disconnection timeouts
        self.election_timeout = random.uniform(HB_PERIOD, DIS_TIMEOUT)
        
        self.election_start_time = None
        self.electing = False
        self.last_leader_heartbeat_time = datetime.now()  # Tracks last heartbeat from leader
        
        self._log(f"Node initialized with peers: {list(self.peers.keys())}")

    def _log(self, message):
        ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        if self.electing:
            election_elapsed = (datetime.now() - self.election_start_time).total_seconds()
            election_timeout = f"{(self.election_timeout - election_elapsed):.3f}s"
        else:
            election_timeout = "N/A"
            
        heartbeat_elapsed = (datetime.now() - self.last_leader_heartbeat_time).total_seconds()
        heartbeat_timeout = f"{(DIS_TIMEOUT - heartbeat_elapsed):.3f}s"
        
        state_info = (
            f"[Term:{self.current_term}] "
            f"[State:{self.state.upper():8}] "
            f"[Leader:{self.leader_id or 'None'}] "
            f"[Commit:{self.commit_index}] "
            f"[Applied:{self.last_applied}] "
            f"[Election:{election_timeout}] "
            f"[Heartbeat:{heartbeat_timeout}]"
        )
        print(f"[{ts}][{self.node_id}] {state_info} {message}")

    async def follower_loop(self):
        """Main loop for follower state."""
        self._log("Entering follower state")
        
        while self.state == 'follower':
            heartbeat_elapsed = (datetime.now() - self.last_leader_heartbeat_time).total_seconds()
            self._log(f"Heartbeat elapsed: {heartbeat_elapsed:.3f}s")
            
            # Check if leader is disconnected based on DIS_TIMEOUT
            if heartbeat_elapsed > DIS_TIMEOUT:
                self._log("Leader connection lost; starting election.")
                elected = await self.start_election()
                
                if elected:
                    await self.become_leader()
                else:
                    self._log("Election failed; retrying in next cycle.")
            
            # Sleep briefly to avoid busy-waiting
            await asyncio.sleep(DIS_TIMEOUT * 2)

    async def RequestVote(self, request, context):
        async with self.lock:
            self._log(f"Received vote request from {request.candidate_id} (term {request.term})")
            
            if request.term < self.current_term:
                self._log("Rejecting vote: stale term")
                return VoteResponse(term=self.current_term, vote_granted=False)
            
            if request.term > self.current_term:
                self.current_term = request.term
                self.voted_for = None
                self.state = 'follower'
                self._log(f"Updated term to {request.term}")

            last_log_term = self.log[-1].term if self.log else 0
            log_ok = (request.last_log_term > last_log_term) or \
                    (request.last_log_term == last_log_term and 
                     request.last_log_index >= len(self.log))
            
            vote_granted = False
            if (self.voted_for is None or self.voted_for == request.candidate_id) and log_ok:
                self.voted_for = request.candidate_id
                self.election_start = datetime.now()
                vote_granted = True
                self._log(f"Granted vote to {request.candidate_id}")
            else:
                self._log(f"Rejecting vote: {'already voted' if self.voted_for else 'log mismatch'}")

            return VoteResponse(
                term=self.current_term,
                vote_granted=vote_granted
            )

    async def AppendEntries(self, request, context):
        async with self.lock:
            self._log(f"Received entries from {request.leader_id} (term {request.term})")
            
            # Process client request of SET command
            if request.leader_id == "client":
                # Only process client requests if we're the leader
                if self.state == 'leader':
                    self._log("Processing client request")
                    # Append new entries to log
                    if request.entries:
                        self._log(f"Received {len(request.entries)} new entries from client")
                        for entry in request.entries:
                            key, value = entry.command.split('=', 1)
                            new_entry = LogEntry(term=self.current_term, command=f"{key.strip()}={value.strip()}")
                            self.log.append(new_entry)
                            self._log(f"Added new entry to log: {new_entry.command}")
                            
                    else:
                        self._log("Received empty entry request from client")
                    
                    return EntryResponse(term=self.current_term, success=True)
                else:
                    self._log("Rejecting client request: not leader")
                    return EntryResponse(term=self.current_term, success=False)
                
            if request.term < self.current_term:
                self._log("Rejecting entries: stale term")
                return EntryResponse(term=self.current_term, success=False)
            
            if request.term > self.current_term:
                self.current_term = request.term
                self.voted_for = None
                self.state = 'follower'
                self._log(f"Updated term to {request.term}")

            self.election_start = datetime.now()
            self.leader_id = request.leader_id
            self.state = 'follower'
            
            # Heartbeat received; update last heartbeat time
            self.last_leader_heartbeat_time = datetime.now()

            # Check for log consistency
            success = True
            if request.prev_log_index > len(self.log):
                self._log(f"Log too short. My length: {len(self.log)}, required index: {request.prev_log_index}")
                success = False
            elif request.prev_log_index > 0 and (len(self.log) < request.prev_log_index or 
                    self.log[request.prev_log_index-1].term != request.prev_log_term):
                self._log(f"Log mismatch at index {request.prev_log_index}")
                success = False

            if success and request.entries:
                self._log(f"Appending {len(request.entries)} entries")
                self.log = self.log[:request.prev_log_index]
                self.log.extend(request.entries)

            if success and request.leader_commit > self.commit_index:
                new_commit = min(request.leader_commit, len(self.log))
                if new_commit > self.commit_index:
                    self.commit_index = new_commit
                    self._log(f"Updated commit index to {new_commit}")

            return EntryResponse(
                term=self.current_term,
                success=success
            )

    async def GetValue(self, request, context):
        async with self.lock:
            self._log(f"Received GET request for key '{request.key}'")
            
            # If we're not the leader, redirect to leader
            if self.state != 'leader':
                # if self.leader_id:
                #     self._log(f"Redirecting GET request to leader {self.leader_id}")
                #     try:
                #         response = await self._rpc_with_retry(
                #             'GetValue',
                #             self.leader_id,
                #             GetRequest(key=request.key)
                #         )
                #         return response
                #     except Exception as e:
                #         self._log(f"Failed to redirect GET request: {str(e)}")
                #         return GetResponse(value="")
                # else:
                #     self._log("No leader known for GET request")
                #     return GetResponse(value="")
                
                self._log("I am not the leader; rejecting GET request")
                return GetResponse(value="")   

            # Process GET request if we're the leader
            value = self.kv_store.get(request.key, "")
            self._log(f"Processed GET request for key '{request.key}' = '{value}'")
            return GetResponse(value=value)
        
    async def ControlConnection(self, request, context):
        async with self.lock:
            self._log(f"Received control connection request: {request}")
            if request.disconnect:
                if request.peer not in self.disconnected:
                    self.disconnected.add(request.peer)
                    self._log(f"Disconnected from {request.peer}")
                    return ConnectionResponse(success=True)
                else:
                    self._log(f"Already disconnected from {request.peer}")
                    return ConnectionResponse(success=False)
            else:
                if request.peer in self.disconnected:
                    self.disconnected.remove(request.peer)
                    self._log(f"Reconnected to {request.peer}")
                    return ConnectionResponse(success=True)
                else:
                    self._log(f"Already connected to {request.peer}")
                    return ConnectionResponse(success=False)

    async def start_election(self):
        """Starts a new election."""
        async with self.lock:
            self._log("Starting election")
            self.state = 'candidate'
            self.current_term += 1
            self.voted_for = self.node_id
            
            # Reset election timer for new term
            self.electing = True
            self.election_start_time = datetime.now()
            
        votes_granted = 1  # Vote for itself
        
        last_log_index = len(self.log)
        last_log_term = self.log[-1].term if last_log_index > 0 else 0

        # Keep track of which peers have voted
        votes_received = {peer: False for peer in self.peers if peer != self.node_id and peer not in self.disconnected}
        
        while self.state == 'candidate':
            # Check if election timeout reached
            election_elapsed = (datetime.now() - self.election_start_time).total_seconds()
            if election_elapsed >= self.election_timeout:
                self._log("Election timeout reached")
                break
            else:
                self._log(f"Election in progress ({election_elapsed:.3f}s elapsed)")

            # Create async task for each peer vote request
            async def request_vote_from_peer(peer):
                if votes_received[peer]:
                    return None
                    
                self._log(f"Requesting vote from {peer}")
                response = await self._rpc_with_retry(
                    'RequestVote', peer,
                    VoteRequest(
                        term=self.current_term,
                        candidate_id=self.node_id,
                        last_log_index=last_log_index,
                        last_log_term=last_log_term,
                    )
                )
                self._log(f"Received vote response from {peer}")
                return peer, response

            # Send vote requests in parallel
            tasks = [request_vote_from_peer(peer) for peer in votes_received.keys()]
            results = await asyncio.gather(*tasks)
            self._log("Vote request round complete")

            # Process results
            for result in results:
                if not result:  # Skip None results
                    continue
                    
                peer, response = result
                self._log(f"Checking vote response from {peer}...")
                if response:
                    self._log(f"Vote granted: {response.vote_granted} from {peer}")
                    
                    if response.vote_granted:
                        votes_received[peer] = True
                        votes_granted += 1
                        
                        # Check if we have majority
                        if votes_granted > len(self.peers) // 2:
                            self._log(f"Votes received: {votes_granted}/{len(self.peers)}")
                            return True
                        else:
                            self._log(f"Votes received: {votes_granted}/{len(self.peers)}")
                    else:
                        self._log(f"Peer {peer} denied vote")
                else:
                    self._log(f"Vote request to {peer} failed")

            # Wait before next round of vote requests
            await asyncio.sleep(DIS_TIMEOUT)
        
        async with self.lock:
            self._log("Election failed - majority not reached")
            # Reset election state
            self.electing = False
            self.election_start_time = None
            
            # If we reach here without becoming leader, go back to follower state
            if self.state == 'candidate':
                self.state = 'follower'
                self._log("Election failed - returning to follower state")
            else:
                self._log("Election failed - state changed during election")
            
        return False

    async def become_leader(self):
        """Transition to leader state."""
        async with self.lock:
            # Reset election state
            self.electing = False
            self.election_start_time = None
            
            self.state = 'leader'
            self.leader_id = self.node_id
            self.next_index = {peer: len(self.log) + 1 for peer in self.peers}
            self.match_index = {peer: 0 for peer in self.peers}
            self._log(f"Elected leader for term {self.current_term}")
            
        await self.leader_loop()

    async def leader_loop(self):
        """Main loop for leader state."""
        self._log("Starting leader duties")
        while self.state == 'leader':
            await self.send_heartbeats()
            await asyncio.sleep(HB_PERIOD)

    async def send_heartbeats(self):
        """Send heartbeats to all peers and update their connection states."""
        # Create a pool of workers for parallel heartbeat sending
        async def send_heartbeat_to_peer(peer):
            next_idx = self.next_index.get(peer, 1)
            prev_log_index = next_idx - 1
            prev_log_term = self.log[prev_log_index - 1].term if prev_log_index > 0 else 0
            entries = self.log[next_idx - 1:] if next_idx <= len(self.log) else []

            response = await self._rpc_with_retry(
                'AppendEntries',
                peer,
                EntryRequest(
                    term=self.current_term,
                    leader_id=self.node_id,
                    prev_log_index=prev_log_index,
                    prev_log_term=prev_log_term,
                    entries=entries,
                    leader_commit=self.commit_index
                )
            )
            return peer, response

        self._log("Sending heartbeats to all peers...")
        # Send heartbeats in parallel using asyncio.gather
        tasks = [send_heartbeat_to_peer(peer) for peer in self.peers if peer != self.node_id and peer not in self.disconnected]
        results = await asyncio.gather(*tasks)
        self._log("Heartbeat round complete")

        # Process results
        for result in results:
            if result:  # Skip None results from self-peer
                peer, response = result
                self._log(f"Received heartbeat response from {peer}...")
                
                if response and response.success:
                    self._log(f"Heartbeat successful for {peer}")
                    
                    next_idx = self.next_index.get(peer, 1)
                    entries = self.log[next_idx - 1:] if next_idx <= len(self.log) else []
                    self.next_index[peer] = next_idx + len(entries)
                    self.match_index[peer] = self.next_index[peer] - 1

        # Update commit index based on majority agreement
        self.update_commit_index()

    def update_commit_index(self):
        """Update the commit index based on match indices."""
        match_indices = sorted(self.match_index.values())
        new_commit = match_indices[len(match_indices) // 2]

        if new_commit > self.commit_index and len(self.log) >= new_commit:
            if self.log[new_commit - 1].term == self.current_term:
                self.commit_index = new_commit
                self._log(f"Advanced commit index to {new_commit}")

    async def apply_commits(self):
        """Apply committed log entries to the key-value store."""
        while True:
            async with self.lock:
                while self.last_applied < self.commit_index:
                    entry = self.log[self.last_applied]
                    key, value = entry.command.split('=', 1)
                    self.kv_store[key] = value
                    self._log(f"Applied command: {key}={value}")
                    self.last_applied += 1
            await asyncio.sleep(COMMIT_PERIOD)
    
    async def _rpc_with_retry(self, method, peer, *args):
        """Handle gRPC calls with retry logic and update connection state."""
        for attempt in range(MAX_RETRIES):
            self._log(f"RPC to {peer} attempt {attempt + 1}")
            try:
                timeout = DIS_TIMEOUT / MAX_RETRIES
                options = [('grpc.enable_http_proxy', 0),
                          ('grpc.keepalive_timeout_ms', int(timeout * 1000))]
                self._log(f"Connecting to {peer} with timeout {timeout:.3f}s")
                
                async with grpc.aio.insecure_channel(f'localhost:{PEER_PORTS[peer]}', options=options) as channel:
                    stub = RaftNodeStub(channel)
                    response = await asyncio.wait_for(getattr(stub, method)(*args), timeout=DIS_TIMEOUT)
                    self._log(f"RPC to {peer} successful")
                    return response
            except grpc.RpcError as e:
                if attempt < MAX_RETRIES - 1:
                    self._log(f"Retrying RPC to {peer}...")
                    continue
                self._log(f"RPC to {peer} failed: {e.code().name} at {e.details()}")
                return None
            except Exception as e:
                self._log(f"Connection to {peer} error: {str(e)}")
                return None

    async def start(self, port):
        """Start the gRPC server and initialize the node."""
        server = grpc.aio.server(futures.ThreadPoolExecutor(max_workers=30))
        add_RaftNodeServicer_to_server(self, server)

        server.add_insecure_port(f'[::]:{port}')
        await server.start()
        self._log(f"gRPC server started on port {port}")

        # Start background tasks
        asyncio.create_task(self.apply_commits())
        
        # Begin follower loop
        await self.follower_loop()

        await server.wait_for_termination()


if __name__ == '__main__':
    node_id = sys.argv[1]
    server = RaftNode(node_id)
    port = PEER_PORTS[node_id]
    asyncio.run(server.start(port))
