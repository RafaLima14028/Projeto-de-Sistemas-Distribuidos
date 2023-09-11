#!/bin/bash
pip install -r requirements.txt
python -m grpc_tools.protoc -I./src --python_out=./src --grpc_python_out=./src src/interface.proto