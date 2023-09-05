import json

import grpc
import interface_pb2  # Import the generated protobuf Python code
import interface_pb2_grpc  # Import the generated gRPC stubs


def run():
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = interface_pb2_grpc.KeyValueStoreStub(channel)

        try:
            # response = stub.Get(interface_pb2.KeyRequest(key="example_key"))
            response = stub.Put(interface_pb2.KeyValueRequest(key="example_key", val='Rafael'))

            print("Get Response:", response)
        except grpc.RpcError as e:
            print(f"Error making gRPC call: {e.code()} - {e.details()}")


def get_range_client():
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = interface_pb2_grpc.KeyValueStoreStub(channel)

        # put_request = interface_pb2.KeyValueRequest(key='A', val='Rafael')
        # print(stub.Put(put_request))

        try:
            responses = stub.GetRange(interface_pb2.KeyRange(
                from_key=interface_pb2.KeyRequest(key='A'),
                to_key=interface_pb2.KeyRequest(key='C')
            ))

            # O problema aparece nesse for
            for response in responses:
                print(response)
        except grpc.RpcError as e:
            print(f"Erro durante a transmissão gRPC: {e}")


def trim_client():
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = interface_pb2_grpc.KeyValueStoreStub(channel)

        put_request = interface_pb2.KeyValueRequest(key='B', val='Rafael')
        print(stub.Put(put_request))

        print('-----')

        try:
            response = stub.Trim(interface_pb2.KeyRequest(
                key='B'
            )
            )

            print(response)
        except grpc.RpcError as e:
            print(f"Erro durante a transmissão gRPC: {e}")


def del_client():
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = interface_pb2_grpc.KeyValueStoreStub(channel)

        put_request = interface_pb2.KeyValueRequest(key='B', val='Rafael')
        print(stub.Put(put_request))

        print('-----')

        try:
            response = stub.Del(interface_pb2.KeyRequest(key='B'))

            print(response)
        except grpc.RpcError as e:
            print(f"Erro durante a transmissão gRPC: {e}")


def del_range_client():
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = interface_pb2_grpc.KeyValueStoreStub(channel)

        print('-----------')

        try:
            responses = stub.DelRange(interface_pb2.KeyRange(
                from_key=interface_pb2.KeyRequest(key='A'),
                to_key=interface_pb2.KeyRequest(key='D')
            ))

            for response in responses:
                print(response)
        except grpc.RpcError as e:
            print(f"Erro durante a transmissão gRPC: {e}")


def get_all_client():
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = interface_pb2_grpc.KeyValueStoreStub(channel)

        try:
            key_request = [
                interface_pb2.KeyRequest(key='D'),
                interface_pb2.KeyRequest(key='A'),
                interface_pb2.KeyRequest(key='Z')
            ]

            responses = stub.GetAll(iter(key_request))

            for response in responses:
                print(response)
        except grpc.RpcError as e:
            print(f"Erro durante a transmissão gRPC: {e}")


if __name__ == '__main__':
    get_all_client()
