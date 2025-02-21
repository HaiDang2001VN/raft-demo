from pbft_server import run_node, stop_all_nodes
from pbft_node import PBFTNode, PBFTNodePrimaryCrash, PBFTNodeByzantine, PBFTNodeNetworkDelay, PBFTNodeTooManyFaults
import time
import grpc
import pbft_pb2
import pbft_pb2_grpc
import hashlib

def hash_block(data):
    return hashlib.sha256(data.encode()).hexdigest()

def send_requests():
    primary_host = "localhost:5000"
    block_data = "Block 1: transaction ABC"
    block_hash = hash_block(block_data)

    with grpc.insecure_channel(primary_host) as channel:
        stub = pbft_pb2_grpc.PBFTServiceStub(channel)
        response = stub.PrePrepare(pbft_pb2.Message(sender="0", block_hash=block_hash))
        print(f"sent PrePrepare with hash {block_hash} from Primary")

    with grpc.insecure_channel(primary_host) as channel:
        stub = pbft_pb2_grpc.PBFTServiceStub(channel)
        response = stub.Commit(pbft_pb2.Message(sender="0", block_hash=block_hash))
        print(f"sent Commit with hash {block_hash} from Primary")

    block_data = "Block 2: transaction XYZ"
    block_hash = hash_block(block_data)

    with grpc.insecure_channel(primary_host) as channel:
        stub = pbft_pb2_grpc.PBFTServiceStub(channel)
        response = stub.PrePrepare(pbft_pb2.Message(sender="0", block_hash=block_hash))
        print(f"sent PrePrepare with hash {block_hash} from Primary")

    with grpc.insecure_channel(primary_host) as channel:
        stub = pbft_pb2_grpc.PBFTServiceStub(channel)
        response = stub.Commit(pbft_pb2.Message(sender="0", block_hash=block_hash))
        print(f"sent Commit with hash {block_hash} from Primary")

def run_normal_scenario():
    peers = ["localhost:5001", "localhost:5002", "localhost:5003"]
    run_node("0", 5000, peers)  
    run_node("1", 5001, peers)
    run_node("2", 5002, peers)
    run_node("3", 5003, peers)

def run_primary_crash_scenario():
    peers = ["localhost:5001", "localhost:5002", "localhost:5003"]
    run_node("0", 5000, peers, node_class=PBFTNodePrimaryCrash)  
    run_node("1", 5001, peers)
    run_node("2", 5002, peers)
    run_node("3", 5003, peers)

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
    run_node("2", 5002, peers)
    run_node("3", 5003, peers, node_class=PBFTNodeTooManyFaults)

def reset_state():
    stop_all_nodes()
    time.sleep(2)  

if __name__ == "__main__":
    # Chạy kịch bản bình thường
    run_normal_scenario()
    time.sleep(5)  
    send_requests()
    reset_state()

    # Chạy kịch bản Primary Crash
    # run_primary_crash_scenario()
    # time.sleep(5)
    # send_requests()
    # reset_state()

    # Chạy kịch bản Byzantine Node
    # run_byzantine_scenario()
    # time.sleep(5)
    # send_requests()
    # reset_state()

    # Chạy kịch bản Network Delay
    # run_network_delay_scenario()
    # time.sleep(5)
    # send_requests()
    # reset_state()

    # Chạy kịch bản Too Many Faults
    # run_too_many_faults_scenario()
    # time.sleep(5)
    # send_requests()
    # reset_state()