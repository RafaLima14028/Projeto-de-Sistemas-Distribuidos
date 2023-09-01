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


if __name__ == '__main__':
    run()
