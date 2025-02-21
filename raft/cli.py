# cli.py
import cmd
import subprocess
import asyncio
import grpc
import signal
from raft_pb2 import *
from raft_pb2_grpc import RaftNodeStub
from config import PEER_PORTS
import threading
from config import MAX_RETRIES, DIS_TIMEOUT

class RaftCLI(cmd.Cmd):
    intro = 'RAFT Cluster Management Console\nType help or ? for commands\n'
    prompt = 'raft> '
    
    def __init__(self):
        super().__init__()
        self.nodes = {}
        self.running = True

    async def execute_command(self, method, peer, *args):
        """Handle gRPC calls with retry logic and update connection state."""
        for attempt in range(MAX_RETRIES):
            print(f"RPC to {peer} attempt {attempt + 1}")
            try:
                timeout = DIS_TIMEOUT / MAX_RETRIES
                options = [('grpc.enable_http_proxy', 0),
                          ('grpc.keepalive_timeout_ms', int(timeout * 1000))]
                print(f"Connecting to {peer} with timeout {timeout:.3f}s")
                
                async with grpc.aio.insecure_channel(f'localhost:{PEER_PORTS[peer]}', options=options) as channel:
                    stub = RaftNodeStub(channel)
                    response = await asyncio.wait_for(getattr(stub, method)(*args), timeout=DIS_TIMEOUT)
                    print(f"RPC to {peer} successful")
                    return response
            except grpc.RpcError as e:
                if attempt < MAX_RETRIES - 1:
                    print(f"Retrying RPC to {peer}...")
                    continue
                print(f"RPC to {peer} failed: {e.code().name} at {e.details()}")
                return None
            except Exception as e:
                print(f"Connection to {peer} error: {str(e)}")
                return None

    async def async_set(self, arg):
        """Set key-value pair"""
        try:
            key, value = arg.split('=', 1)
            key = key.strip()
            value = value.strip()
            
            # Create tasks for all nodes
            tasks = []
            for peer in PEER_PORTS.keys():
                tasks.append(self.execute_command(
                    'AppendEntries', peer,
                    EntryRequest(
                        term=0,
                        leader_id="client",
                        prev_log_index=0,
                        prev_log_term=0,
                        entries=[LogEntry(term=0, command=f"{key}={value}")],
                        leader_commit=0
                    )
                ))
            
            # Run all tasks concurrently
            responses = await asyncio.gather(*tasks)
            
            # Filter for successful leader response
            success = False
            for peer, response in zip(PEER_PORTS.keys(), responses):
                if response and response.success:
                    success = True
                    print(f"Leader {peer} accepted the request")
                    # break
            
            if not success:
                raise Exception("No leader found or request failed")
            print(f"Set {key}={value}")
        except ValueError:
            print("Usage: SET key=value")
        except Exception as e:
            print(f"Failed to set value: {str(e)}")

    def do_set(self, arg):
        """Set key-value: SET key=value"""
        asyncio.run(self.async_set(arg))

    async def async_get(self, key):
        """Get value for key"""
        try:
            # Create tasks for all nodes
            tasks = []
            for peer in PEER_PORTS.keys():
                tasks.append(
                    self.execute_command('GetValue', peer, GetRequest(key=key))
                )
            
            # Run all tasks concurrently
            responses = await asyncio.gather(*tasks)
            
            # Check for successful responses
            for peer, response in zip(PEER_PORTS.keys(), responses):
                if response and response.value:
                    print(f"From leader {peer}: {key} = {response.value}")
                    # return
            else:
                print(f"Key '{key}' not found")
        except Exception as e:
            print(f"Failed to get value: {str(e)}")

    def do_get(self, arg):
        """Get value: GET <key>"""
        if not arg:
            print("Usage: GET <key>")
            return
        asyncio.run(self.async_get(arg.strip()))

    def do_exit(self, line):
        """Exit the console"""
        self.running = False
        return True
    
    # Create async function for toggle
    async def async_toggle(self, node1, node2, disconnect=False):
        tasks = [
            self.execute_command(
                'ControlConnection',
                node1,
                ConnectionRequest(
                    peer=node2, disconnect=disconnect)
                ),
            self.execute_command(
                'ControlConnection',
                node2,
                ConnectionRequest(
                    peer=node1, disconnect=disconnect)
                )
            ]

        responses = await asyncio.gather(*tasks)
        if all(r and r.success for r in responses):
            print(
                f"Successfully toggled connection between {node1} and {node2}")
        else:
            print(
                f"Failed to toggle connection between {node1} and {node2}")
            
    def do_connect(self, line):
        """Connect two nodes: CONNECT <node1> <node2>"""
        try:
            node1, node2 = line.split()
            if node1 not in PEER_PORTS or node2 not in PEER_PORTS:
                print("Invalid node names")
                return
            
            print(f"Connecting {node1} and {node2}")
            # Run the async function
            asyncio.run(self.async_toggle(node1, node2, disconnect=False))
        except ValueError:
            print("Usage: CONNECT <node1> <node2>")
            
    def do_disconnect(self, line):
        """Disconnect two nodes: DISCONNECT <node1> <node2>"""
        try:
            node1, node2 = line.split()
            if node1 not in PEER_PORTS or node2 not in PEER_PORTS:
                print("Invalid node names")
                return
            
            print(f"Disconnecting {node1} and {node2}")
            # Run the async function
            asyncio.run(self.async_toggle(node1, node2, disconnect=True))
        except ValueError:
            print("Usage: DISCONNECT <node1> <node2>")
    
    def default(self, line):
        print(f"Unknown command: {line}")
        print("Type 'help' for available commands")

if __name__ == '__main__':
    try:
        RaftCLI().cmdloop()
    except KeyboardInterrupt:
        print("\nExiting...")
