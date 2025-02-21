import sqlite3

class PBFTDatabase:
    def __init__(self, db_name="pbft.db"):
        self.conn = sqlite3.connect(db_name, check_same_thread=False)
        self.cursor = self.conn.cursor()
        self.create_table()

    def create_table(self):
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS blockchain (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            block_hash TEXT UNIQUE,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")
        self.conn.commit()

    def add_block(self, block_hash):
        self.cursor.execute("INSERT OR IGNORE INTO blockchain (block_hash) VALUES (?)", (block_hash,))
        self.conn.commit()

    def get_blocks(self):
        self.cursor.execute("SELECT * FROM blockchain")
        return self.cursor.fetchall()

# filepath: pbft.proto
"""
syntax = "proto3";

service PBFTService {
    rpc PrePrepare (Message) returns (Response);
    rpc Prepare (Message) returns (Response);
    rpc Commit (Message) returns (Response);
}

message Message {
    string sender = 1;
    string block_hash = 2;
}

message Response {
    bool success = 1;
}
"""

import grpc
from concurrent import futures
import pbft_pb2
import pbft_pb2_grpc
import hashlib
import time

class PBFTNode(pbft_pb2_grpc.PBFTServiceServicer):
    def __init__(self, id, peers, is_primary=False):
        self.id = id
        self.peers = peers
        self.is_primary = is_primary
        self.blockchain = PBFTDatabase()
        self.messages = {"prepare": [], "commit": []} 

    def hash_block(self, block_data):
        return hashlib.sha256(block_data.encode()).hexdigest()

    def PrePrepare(self, request, context):
        print(f"[{self.id}] receive PrePrepare from {request.sender} with hash {request.block_hash}")
        
        for peer in self.peers:
            with grpc.insecure_channel(peer) as channel:
                stub = pbft_pb2_grpc.PBFTServiceStub(channel)
                response = stub.Prepare(pbft_pb2.Message(sender=self.id, block_hash=request.block_hash))
                if response.success:
                    self.messages["prepare"].append(request.block_hash)
        
        return pbft_pb2.Response(success=True)

    def Prepare(self, request, context):
        print(f"[{self.id}] receive Prepare from {request.sender} w hash {request.block_hash}")
        self.messages["prepare"].append(request.block_hash)

        if self.messages["prepare"].count(request.block_hash) >= 2:
            for peer in self.peers:
                with grpc.insecure_channel(peer) as channel:
                    stub = pbft_pb2_grpc.PBFTServiceStub(channel)
                    response = stub.Commit(pbft_pb2.Message(sender=self.id, block_hash=request.block_hash))
                    if response.success:
                        self.messages["commit"].append(request.block_hash)

        return pbft_pb2.Response(success=True)

    def Commit(self, request, context):
        print(f"[{self.id}] receive Commit from {request.sender} with hash {request.block_hash}")
        self.messages["commit"].append(request.block_hash)

        if self.messages["commit"].count(request.block_hash) >= 2:
            self.blockchain.add_block(request.block_hash)
            print(f"[{self.id}] Block {request.block_hash} commited to database!")

        return pbft_pb2.Response(success=True)

def serve(port, node_id, peers, is_primary=False, node_class=PBFTNode):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    node = node_class(node_id, peers, is_primary)  # Đảm bảo `node_class` là một class hợp lệ
    pbft_pb2_grpc.add_PBFTServiceServicer_to_server(node, server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    print(f"[Node {node_id}] Running on port {port}")
    server.wait_for_termination()

import threading

def run_node(node_id, port, peers, node_class=PBFTNode, is_primary=False):
    threading.Thread(target=serve, args=(port, node_id, peers, is_primary, node_class)).start()

peers = ["localhost:5001", "localhost:5002", "localhost:5003"]
#Chạy trường hợp bình thường - không lỗi
run_node("0", 5000, peers)  
run_node("1", 5001, peers)
run_node("2", 5002, peers)
run_node("3", 5003, peers)

import grpc
import pbft_pb2
import pbft_pb2_grpc
import hashlib

def hash_block(data):
    return hashlib.sha256(data.encode()).hexdigest()

primary_host = "localhost:5000" 
block_data = "Block 1: transaction ABC"
block_hash = hash_block(block_data)

with grpc.insecure_channel(primary_host) as channel:
    stub = pbft_pb2_grpc.PBFTServiceStub(channel)
    response = stub.PrePrepare(pbft_pb2.Message(sender="0", block_hash=block_hash))

print(f"sent block with hash {block_hash} from Primary")

import grpc
import pbft_pb2
import pbft_pb2_grpc
import hashlib

def hash_block(data):
    return hashlib.sha256(data.encode()).hexdigest()

primary_host = "localhost:5000"
block_data = "Block 2: transaction XYZ"
block_hash = hash_block(block_data)

with grpc.insecure_channel(primary_host) as channel:
    stub = pbft_pb2_grpc.PBFTServiceStub(channel)
    response = stub.PrePrepare(pbft_pb2.Message(sender="0", block_hash=block_hash))

print(f"sent block with hash {block_hash} from Primary")

#Mô phòng trường hợp lỗi : Node chính bị crash
class PBFTNodePrimaryCrash(PBFTNode):
    def PrePrepare(self, request, context):
        print(f"[Node {self.id}] Primary node crashed, no response")
        return pbft_pb2.Response(success=False)

#Mô phòng trường hợp : Byzantine node - gửi dữ liệu sai
class PBFTNodeByzantine(PBFTNode):
    def PrePrepare(self, request, context):
        print(f"[Node {self.id}] sending incorrect block hash (byzantine fault)")
        wrong_hash = self.hash_block("fake dataa")
        return pbft_pb2.Response(success=False)

#Mô phòng trường hợp : mạng có vấn đề: mất gói hoặc delay
import random

class PBFTNodeNetworkDelay(PBFTNode):
    def Prepare(self, request, context):
        if random.random() < 0.5:
            print(f"[Node {self.id}] dropping message due to network delay")
            return pbft_pb2.Response(success=False)
        return super().Prepare(request, context)

    def Commit(self, request, context):
        if random.random() < 0.5:
            print(f"[Node {self.id}] delayed commit message (network delay)")
            return pbft_pb2.Response(success=False)
        return super().Commit(request, context)

#Mô phòng trường hợp : nhiều lỗi byzantine - f > n/3
class PBFTNodeTooManyFaults(PBFTNode):
    def Commit(self, request, context):
        print(f"[Node {self.id}] too many byzantine faults ==> commit rejected")
        return pbft_pb2.Response(success=False)

# run_node("0", 5000, peers, node_class=PBFTNodePrimaryCrash)  # Primary Crash
# run_node("2", 5002, peers, node_class=PBFTNodeByzantine)  # Byzantine Node
# run_node("1", 5001, peers, node_class=PBFTNodeNetworkDelay)  # Network Delay
# run_node("3", 5003, peers, node_class=PBFTNodeTooManyFaults)  # Too many faults
run_node("0", 5000, peers, node_class=PBFTNodePrimaryCrash)  
run_node("1", 5001, peers)
run_node("2", 5002, peers)
run_node("3", 5003, peers)