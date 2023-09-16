#!/bin/bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python3 -m grpc_tools.protoc -I./src --python_out=./src --grpc_python_out=./src src/interface.proto
