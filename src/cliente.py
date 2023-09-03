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

        responses = stub.GetRange(interface_pb2.KeyRange(
            from_key=interface_pb2.KeyRequest(key='A'),
            to_key=interface_pb2.KeyRequest(key='C')
        ))
        # responses = list(iter(responses))

        # for response in responses:
        #     print(response.key)

        #
        for response in responses:
            print(response)


if __name__ == '__main__':
    # run()
    get_range_client()
