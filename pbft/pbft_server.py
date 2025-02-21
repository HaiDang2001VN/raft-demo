import grpc
from concurrent import futures
import threading
from pbft_node import PBFTNode, PBFTNodePrimaryCrash, PBFTNodeByzantine, PBFTNodeNetworkDelay, PBFTNodeTooManyFaults
import pbft_pb2_grpc

servers = []

def serve(port, node_id, peers, is_primary=False, node_class=PBFTNode):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    node = node_class(node_id, peers, is_primary)  
    pbft_pb2_grpc.add_PBFTServiceServicer_to_server(node, server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    servers.append(server)
    print(f"[Node {node_id}] Running on port {port}")
    server.wait_for_termination()

def run_node(node_id, port, peers, node_class=PBFTNode, is_primary=False):
    threading.Thread(target=serve, args=(port, node_id, peers, is_primary, node_class)).start()

def stop_all_nodes():
    for server in servers:
        server.stop(0)
    servers.clear()
