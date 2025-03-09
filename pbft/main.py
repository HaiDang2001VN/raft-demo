from pbft_server import run_node, stop_all_nodes
from pbft_node import PBFTNode, PBFTNodePrimaryCrash, PBFTNodeByzantine, PBFTNodeNetworkDelay, PBFTNodeViewChange
import time
import grpc
import pbft_pb2
import pbft_pb2_grpc
import hashlib

def hash_block(data):
    return hashlib.sha256(data.encode()).hexdigest()

def send_requests():
    primary_host = "localhost:5000"
    all_nodes = ["localhost:5000", "localhost:5001", "localhost:5002", "localhost:5003"]
    block_data = "Block 1: transaction ABC"
    block_hash = hash_block(block_data)

    print("\n--- Sending requests from client to nodes ---")
    
    # Send PrePrepare requests to all nodes
    for node in all_nodes:
        try:
            with grpc.insecure_channel(node) as channel:
                stub = pbft_pb2_grpc.PBFTServiceStub(channel)
                response = stub.PrePrepare(pbft_pb2.Message(sender="client", block_hash=block_hash))
        except Exception as e:
            print(f"Failed to send PrePrepare to {node}: {e}")

    time.sleep(5) 

    # # Send Commit requests to all nodes
    # for node in all_nodes:
    #     try:
    #         with grpc.insecure_channel(node) as channel:
    #             stub = pbft_pb2_grpc.PBFTServiceStub(channel)
    #             response = stub.Commit(pbft_pb2.Message(sender="client", block_hash=block_hash))
    #     except Exception as e:
    #         print(f"Failed to send Commit to {node}: {e}")

def run_normal_scenario():
    peers = ["localhost:5001", "localhost:5002", "localhost:5003"]
    run_node("0", 5000, peers)  
    run_node("1", 5001, peers)
    run_node("2", 5002, peers)
    run_node("3", 5003, peers)

def run_primary_crash_scenario():
    peers = ["localhost:5001", "localhost:5002", "localhost:5003"]
    run_node("0", 5000, peers, node_class=PBFTNodeViewChange, is_primary=True)  
    run_node("1", 5001, peers, node_class=PBFTNodeViewChange)
    run_node("2", 5002, peers, node_class=PBFTNodeViewChange)
    run_node("3", 5003, peers, node_class=PBFTNodeViewChange)

def run_byzantine_scenario():
    peers = ["localhost:5001", "localhost:5002", "localhost:5003"]
    run_node("0", 5000, peers)  
    run_node("1", 5001, peers)
    run_node("2", 5002, peers, node_class=PBFTNodeByzantine)
    run_node("3", 5003, peers)

def run_network_delay_scenario():
    peers = ["localhost:5001", "localhost:5002", "localhost:5003"]
    run_node("0", 5000, peers)  
    run_node("1", 5001, peers, node_class=PBFTNodeNetworkDelay)
    run_node("2", 5002, peers)
    run_node("3", 5003, peers)

def run_too_many_faults_scenario():
    peers = ["localhost:5001", "localhost:5002", "localhost:5003"]
    run_node("0", 5000, peers)  
    run_node("1", 5001, peers)
    run_node("2", 5002, peers, node_class=PBFTNodeByzantine)
    run_node("3", 5003, peers, node_class=PBFTNodeByzantine)

def reset_state():
    stop_all_nodes()
    time.sleep(2)  

def Commit(self, request, context):
    if self.blockchain.has_block(request.block_hash):
        return pbft_pb2.Response(success=True)
        
    self.messages["commit"].append(request.block_hash)

    if self.messages["commit"].count(request.block_hash) >= 2:
        self.blockchain.add_block(request.block_hash)
        print(f"[Node {self.id}] Block {request.block_hash[:8]}... committed to database!")
        
        # Clean up messages after commit
        self.messages["commit"] = [h for h in self.messages["commit"] if h != request.block_hash]
        self.messages["prepare"] = [h for h in self.messages["prepare"] if h != request.block_hash]

    return pbft_pb2.Response(success=True)

def has_block(self, block_hash):
    return block_hash in self.blocks

if __name__ == "__main__":
    # run_primary_crash_scenario()
    # time.sleep(10)  
    # send_requests()
    # reset_state()

    # run_normal_scenario()
    # time.sleep(5)
    # send_requests()
    # reset_state()

    # run_byzantine_scenario()
    # time.sleep(5)
    # send_requests()
    # reset_state()

    # run_network_delay_scenario()
    # time.sleep(5)
    # send_requests()
    # reset_state()

    run_too_many_faults_scenario()
    time.sleep(5)
    send_requests()
    reset_state()