#!/bin/bash
# mosquitto local deve estar inicializado
source venv/bin/activate
python ./src/servidor.py 50051
