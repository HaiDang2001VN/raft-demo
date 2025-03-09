import grpc
from concurrent import futures
import pbft_pb2
import pbft_pb2_grpc
import hashlib
import time
import random
import threading
from pbft_database import PBFTDatabase

VERBOSE_LOGGING = False

class PBFTNode(pbft_pb2_grpc.PBFTServiceServicer):
    def __init__(self, id, peers, is_primary=False):
        self.id = id
        self.peers = peers
        self.is_primary = is_primary
        self.blockchain = PBFTDatabase()
        self.messages = {"prepare": [], "commit": []}
        self.commit_messages = []

    def hash_block(self, block_data):
        return hashlib.sha256(block_data.encode()).hexdigest()

    def PrePrepare(self, request, context):
        if VERBOSE_LOGGING:
            print(f"[{self.id}] receive PrePrepare from {request.sender} with hash {request.block_hash}")
        else:
            print(f"[Node {self.id}] Processing PrePrepare for block {request.block_hash[:8]}...")
        
        for peer in self.peers:
            with grpc.insecure_channel(peer) as channel:
                stub = pbft_pb2_grpc.PBFTServiceStub(channel)
                response = stub.Prepare(pbft_pb2.Message(sender=self.id, block_hash=request.block_hash))
                if response.success:
                    self.messages["prepare"].append(request.block_hash)
        
        return pbft_pb2.Response(success=True)

    def Prepare(self, request, context):
        if VERBOSE_LOGGING:
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
        if self.blockchain.has_block(request.block_hash):
            return pbft_pb2.Response(success=True)
        
        if request.block_hash not in self.messages["commit"]:
            self.messages["commit"].append(request.block_hash)
        
        if self.messages["commit"].count(request.block_hash) >= 3:
            self.blockchain.add_block(request.block_hash)
            print(f"[Node {self.id}] Block {request.block_hash} committed to database!")
        else:
            print(f"[Node {self.id}] Block {request.block_hash} rejected!")
            
        return pbft_pb2.Response(success=True)

class PBFTNodePrimaryCrash(PBFTNode):
    def PrePrepare(self, request, context):
        print(f"[Node {self.id}] Primary node crashed, no response")
        return pbft_pb2.Response(success=False)

class PBFTNodeByzantine(PBFTNode):
    def PrePrepare(self, request, context):
        print(f"[Node {self.id}] BYZANTINE FAULT: sending incorrect block hash")
        return pbft_pb2.Response(success=False)
        
    def Prepare(self, request, context):
        print(f"[Node {self.id}] BYZANTINE FAULT: corrupting prepare phase")
        return pbft_pb2.Response(success=False)
        
    def Commit(self, request, context):
        print(f"[Node {self.id}] BYZANTINE FAULT: rejecting commit")
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

class PBFTNodeViewChange(PBFTNode):
    def __init__(self, id, peers, is_primary=False):
        super().__init__(id, peers, is_primary)
        self.view_number = 0
        self.timeout = 5  
        self.last_message_time = time.time()
        self.running = True
        self.timeout_thread = threading.Thread(target=self.check_timeout)
        self.timeout_thread.daemon = True
        self.timeout_thread.start()
        
    def check_timeout(self):
        while self.running:
            if not self.is_primary: 
                if time.time() - self.last_message_time > self.timeout:
                    self.initiate_view_change()
                    time.sleep(self.timeout) 
            time.sleep(1)
        
    def initiate_view_change(self):
        print(f"[Node {self.id}] Initiating View Change - timeout detected")
        self.view_number += 1
        new_primary = self.view_number % (len(self.peers) + 1)
        if str(new_primary) == self.id:
            self.is_primary = True
            print(f"[Node {self.id}] is now the new Primary (view {self.view_number})")
            self.broadcast_new_primary()
    
    def broadcast_new_primary(self):
        for peer in self.peers:
            try:
                with grpc.insecure_channel(peer) as channel:
                    stub = pbft_pb2_grpc.PBFTServiceStub(channel)
                    stub.Commit(pbft_pb2.Message(
                        sender=self.id,
                        block_hash=f"view_change_{self.view_number}"
                    ))
            except Exception as e:
                print(f"Error broadcasting new primary to {peer}: {e}")

    def PrePrepare(self, request, context):
        self.last_message_time = time.time()
        if self.is_primary:
            print(f"[Node {self.id}] Primary node crashed, no response")
            return pbft_pb2.Response(success=False)
        else:
            return pbft_pb2.Response(success=True)

    def Prepare(self, request, context):
        self.last_message_time = time.time()
        return super().Prepare(request, context)

    def Commit(self, request, context):
        self.last_message_time = time.time()
        if "view_change" in request.block_hash:
            print(f"[Node {self.id}] Received view change notification from Node {request.sender}")
            return pbft_pb2.Response(success=True)
        return super().Commit(request, context)