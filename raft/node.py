import grpc
# multiprocessing Pool
from multiprocessing import Pool as mp
import time
import random

import raft_pb2
import raft_pb2_grpc
from concurrent.futures import ThreadPoolExecutor, TimeoutError

class RaftNode(raft_pb2_grpc.RaftServicer):
    def __init__(self, node_id, peers):
        self.node_id = node_id
        self.current_term = 0
        self.voted_for = None
        self.log = []
        self.commit_index = 0
        self.last_applied = 0
        self.next_index = {}
        self.match_index = {}
        self.state = 'follower'
        self.leader_id = None
        self.peers = peers
        self.resolution = 0.1  # seconds
        self.max_failures = 3  # threshold for marking a peer as offline temporarily
        self.last_heartbeat = None
        self.free_timeout = None

    def random_timeout(self):
        return random.uniform(0.15, 0.3)
    
    def start(self):
        self.reset_timeout()
        self.run()
        
    def run(self):
        while True:
            if self.state != 'leader' and (time.time() - self.last_heartbeat) > self.free_timeout:
                self.start_election()
            elif self.state == 'leader':
                self.replicate_log()
                
            time.sleep(self.resolution)
            
    def reset_timeout(self):
        # Timeouts and locks
        self.last_heartbeat = time.time()
        self.free_timeout = self.random_timeout()

    def request_vote_from_peer(self, peer, timeout):
        try:
            with grpc.insecure_channel(peer) as channel:
                stub = raft_pb2_grpc.RaftStub(channel)
                request = raft_pb2.RequestVoteRequest(
                    term=self.current_term,
                    candidateId=self.node_id,
                    lastLogIndex=len(self.log) - 1,
                    lastLogTerm=self.log[-1].term if self.log else 0
                )
                
                with ThreadPoolExecutor() as executor:
                    future = executor.submit(stub.RequestVote, request)
                    response = future.result(timeout=timeout)
                
                if response.voteGranted:
                    return (1, 1)
                else:
                    return (0, 1)
        except (Exception, TimeoutError) as e:
            print(f"Error contacting peer {peer}: {e}")
            return (0, 0)

    def start_election(self):
        self.state = 'candidate'
        election_timeout = self.random_timeout()
        
        self.current_term += 1
        self.voted_for = self.node_id
        votes_received = 1
        total_votes = 1

        with mp(processes=len(self.peers)) as pool:
            results = pool.starmap(self.request_vote_from_peer, [(peer, election_timeout) for peer in self.peers])
            counted_votes = map(sum, zip(*results))
            votes_received += counted_votes[0]
            total_votes += counted_votes[1]

        if votes_received > total_votes // 2:
            self.state = 'leader'
            self.leader_id = self.node_id
            print(f"Node {self.node_id} became the leader for term {self.current_term}")

            for peer in self.peers:
                self.next_index[peer] = len(self.log)
                self.match_index[peer] = 0

    def RequestVote(self, request, context):
        if request.term < self.current_term:
            return raft_pb2.RequestVoteResponse(term=self.current_term, voteGranted=False)

        if request.term > self.current_term:
            self.current_term = request.term
            self.voted_for = None
            self.state = 'follower'
            self.leader_id = None

        if (self.voted_for is None or self.voted_for == request.candidateId):
            # Simple log check: if candidate's log is as up to date
            # as receiver's log, grant vote
            last_index = len(self.log) - 1
            last_term = self.log[last_index].term if last_index >= 0 else 0
            up_to_date = (request.lastLogTerm > last_term) or \
                            (request.lastLogTerm == last_term and request.lastLogIndex >= last_index)
            if up_to_date:
                self.voted_for = request.candidateId
                return raft_pb2.RequestVoteResponse(term=self.current_term, voteGranted=True)
        return raft_pb2.RequestVoteResponse(term=self.current_term, voteGranted=False)

    def replicate_to(self, peer):
        try:
            with grpc.insecure_channel(peer) as channel:
                stub = raft_pb2_grpc.RaftStub(channel)
                prev_log_index = self.next_index[peer] - 1
                prev_log_term = self.log[prev_log_index].term if prev_log_index >= 0 else 0
                entries = self.log[self.next_index[peer]:]
                
                request = raft_pb2.AppendEntriesRequest(
                    term=self.current_term,
                    leaderId=self.node_id,
                    prevLogIndex=prev_log_index,
                    prevLogTerm=prev_log_term,
                    entries=entries,
                    leaderCommit=self.commit_index
                )
                
                with ThreadPoolExecutor() as executor:
                    future = executor.submit(stub.AppendEntries, request)
                    response = future.result(timeout=self.resolution)
                
                if response.success:
                    self.next_index[peer] = len(self.log)
                    self.match_index[peer] = len(self.log) - 1
                    return True
                else:
                    self.next_index[peer] = max(0, self.next_index[peer] - 1)
                    return False
        except Exception as e:
            print(f"Error replicating log to peer {peer}: {e}")
            return False

    def replicate_log(self):
        with mp(processes=len(self.peers)) as pool:
            pool.map(self.replicate_to, self.peers)

    def AppendEntries(self, request, context):
        response = raft_pb2.AppendEntriesResponse(term=self.current_term, success=False)
        
        if request.term < self.current_term:
            return response
        
        # This node recognizes the current leader
        self.current_term = request.term
        self.leader_id = request.leaderId
        self.reset_timeout()
        self.state = 'follower'
        response.success = True

        # Log consistency check
        if request.prevLogIndex >= len(self.log):
            response.success = False
            return response
        if request.prevLogIndex >= 0 and \
            self.log[request.prevLogIndex].term != request.prevLogTerm:
            response.success = False
            return response
        
        # Append new entries
        if request.entries:
            self.log = self.log[:request.prevLogIndex + 1] + request.entries
            # TODO: consume entries
            if request.leaderCommit > self.commit_index:
                self.commit_index = min(request.leaderCommit, len(self.log) - 1)
        else:
            if request.leaderCommit > self.commit_index:
                self.commit_index = request.leaderCommit

        return response

    def ClientCommand(self, request, context):
        with self.lock:
            # Just a placeholder example
            response = raft_pb2.ClientCommandResponse(success=False, result="")
            if self.state != 'leader':
                response.result = f"Node {self.node_id} is not the leader."
                return response

            self.consume_entry(request.command)
            response.success = True
            response.result = f"Command {request.command} appended."
            return response

    def consume_entry(self, entry):
        new_entry = raft_pb2.LogEntry(term=self.current_term, command=entry)
        self.log.append(new_entry)
        self.commit_index = len(self.log) - 1