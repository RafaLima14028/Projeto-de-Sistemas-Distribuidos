#!/bin/bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. src/proto/interface.proto
