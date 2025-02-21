import grpc
from concurrent import futures
import pbft_pb2
import pbft_pb2_grpc
import hashlib
import time
import random
from pbft_database import PBFTDatabase

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

class PBFTNodePrimaryCrash(PBFTNode):
    def PrePrepare(self, request, context):
        print(f"[Node {self.id}] Primary node crashed, no response")
        return pbft_pb2.Response(success=False)

class PBFTNodeByzantine(PBFTNode):
    def PrePrepare(self, request, context):
        print(f"[Node {self.id}] sending incorrect block hash (byzantine fault)")
        wrong_hash = self.hash_block("fake dataa")
        return pbft_pb2.Response(success=False)

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

class PBFTNodeTooManyFaults(PBFTNode):
    def Commit(self, request, context):
        print(f"[Node {self.id}] too many byzantine faults ==> commit rejected")
        return pbft_pb2.Response(success=False)