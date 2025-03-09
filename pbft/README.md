python -m venv venv
venv\Scripts\activate
pip install grpcio grpcio-tools
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. pbft.proto
python main.py