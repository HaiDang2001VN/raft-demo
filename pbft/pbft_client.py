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