import json

import grpc
import interface_pb2  # Import the generated protobuf Python code
import interface_pb2_grpc  # Import the generated gRPC stubs


def input_and_update(port: int = 50051) -> None:
    key_read = input('Enter key to insert/update: ')
    value_read = input('Enter the value for the key: ')

    with grpc.insecure_channel(f'localhost:{port}') as channel:
        stub = interface_pb2_grpc.KeyValueStoreStub(channel)

        try:
            put_request = interface_pb2.KeyValueRequest(key=key_read, val=value_read)

            reply = stub.Put(put_request)

            print()
            print('Response:')
            print(f'{reply}')
        except grpc.RpcError as e:
            print(f"Error during gRPC transmission: {e}")


def get(port: int = 50051) -> None:
    key_read = input('Enter the key for the search: ')
    print('If you want the latest version, dont put any version')
    version_read = input('Enter version to search: ')

    if version_read != '':
        version_read = int(version_read)
    else:
        version_read = -1

    with grpc.insecure_channel(f'localhost:{port}') as channel:
        stub = interface_pb2_grpc.KeyValueStoreStub(channel)

        try:
            reply = stub.Get(interface_pb2.KeyRequest(key=key_read, ver=version_read))

            if reply.ver <= 0:
                print('Key or version is not present')
            else:
                print()
                print('Response:')
                print(reply)
        except grpc.RpcError as e:
            print(f"Error during gRPC transmission: {e}")


def delete(port: int = 50051) -> None:
    key_read = input('Enter the key you want to remove: ')

    with grpc.insecure_channel(f'localhost:{port}') as channel:
        stub = interface_pb2_grpc.KeyValueStoreStub(channel)

        try:
            reply = stub.Del(interface_pb2.KeyValueRequest(key=key_read))

            if reply.ver <= 0:
                print('This key has already been removed')
            else:
                print()
                print('Response:')
                print(reply)
        except grpc.RpcError as e:
            print(f"Error during gRPC transmission: {e}")


# TODO: Retornar mais tuplas, está retornando apenas a última
def delete_range(port: int = 50051) -> None:
    from_key_read = input('Enter the initial key you want to remove: ')
    end_key_read = input('Enter the end key you want to remove: ')

    with grpc.insecure_channel(f'localhost:{port}') as channel:
        stub = interface_pb2_grpc.KeyValueStoreStub(channel)

        try:
            replys = stub.DelRange(interface_pb2.KeyRange(
                from_key=interface_pb2.KeyRequest(key=from_key_read),
                to_key=interface_pb2.KeyRequest(key=end_key_read)
            ))

            print()
            print('Response:')
            for reply in replys:
                print(reply)
        except grpc.RpcError as e:
            print(f"Error during gRPC transmission: {e}")


def trim(port: int = 50051) -> None:
    key_read = input('Enter the key you want to remove: ')

    with grpc.insecure_channel(f'localhost:{port}') as channel:
        stub = interface_pb2_grpc.KeyValueStoreStub(channel)

        try:
            reply = stub.Trim(interface_pb2.KeyRequest(key=key_read))

            if reply.ver <= 0:
                print('This key has already been removed')
            else:
                print()
                print('Response:')
                print(reply)
        except grpc.RpcError as e:
            print(f"Error during gRPC transmission: {e}")


# def get_range_client():
#     with grpc.insecure_channel('localhost:50051') as channel:
#         stub = interface_pb2_grpc.KeyValueStoreStub(channel)
#
#         try:
#             responses = stub.GetRange(interface_pb2.KeyRange(
#                 from_key=interface_pb2.KeyRequest(key='A'),
#                 to_key=interface_pb2.KeyRequest(key='C')
#             ))
#
#             for response in responses:
#                 print(response)
#         except grpc.RpcError as e:
#             print(f"Error during gRPC transmission: {e}")
#
#
# def trim_client():
#     with grpc.insecure_channel('localhost:50051') as channel:
#         stub = interface_pb2_grpc.KeyValueStoreStub(channel)
#
#         put_request = interface_pb2.KeyValueRequest(key='B', val='Rafael')
#         print(stub.Put(put_request))
#
#         print('-----')
#
#         try:
#             response = stub.Trim(interface_pb2.KeyRequest(
#                 key='B'
#             )
#             )
#
#             print(response)
#         except grpc.RpcError as e:
#             print(f"Error during gRPC transmission: {e}")
#
#
#
#
# def del_range_client():
#     with grpc.insecure_channel('localhost:50051') as channel:
#         stub = interface_pb2_grpc.KeyValueStoreStub(channel)
#
#         print('-----------')
#
#         try:
#             responses = stub.DelRange(interface_pb2.KeyRange(
#                 from_key=interface_pb2.KeyRequest(key='A'),
#                 to_key=interface_pb2.KeyRequest(key='D')
#             ))
#
#             for response in responses:
#                 print(response)
#         except grpc.RpcError as e:
#             print(f"Error during gRPC transmission: {e}")
#
#
# def get_all_client():
#     with grpc.insecure_channel('localhost:50051') as channel:
#         stub = interface_pb2_grpc.KeyValueStoreStub(channel)
#
#         try:
#             key_request = [
#                 interface_pb2.KeyRequest(key='D'),
#                 interface_pb2.KeyRequest(key='A'),
#                 interface_pb2.KeyRequest(key='Z')
#             ]
#
#             responses = stub.GetAll(iter(key_request))
#
#             for response in responses:
#                 print(response)
#         except grpc.RpcError as e:
#             print(f"Error during gRPC transmission: {e}")
#
#
# def del_all_client():
#     with grpc.insecure_channel('localhost:50051') as channel:
#         stub = interface_pb2_grpc.KeyValueStoreStub(channel)
#
#         try:
#             key_request = [
#                 interface_pb2.KeyRequest(key='D'),
#                 interface_pb2.KeyRequest(key='A'),
#                 interface_pb2.KeyRequest(key='Z')
#             ]
#
#             responses = stub.DelAll(iter(key_request))
#
#             for response in responses:
#                 print(response)
#         except grpc.RpcError as e:
#             print(f"Error during gRPC transmission: {e}")


def options() -> None:
    print()
    print('###############')
    print('1 - Update/insert entered value and key')
    print('2 - Returns value by key and version')
    print('3 - Removes all values associated with the key')
    print('4 - Removes all values associated with the key, except the latest version')
    print('5 - Removes values in the range between the two keys')
    print('###############')
    print()


if __name__ == '__main__':
    while True:
        options()
        option = int(input('Please enter a valid option: '))

        match option:
            case 1:
                input_and_update()
            case 2:
                get()
            case 3:
                delete()
            case 4:
                trim()
            case 5:
                delete_range()
            case _:
                print('This option is not valid!')
