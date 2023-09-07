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

def put_all(port: int = 50051) -> None:
    key_value_read = []

    while True:
        key_read = input('Enter key to insert/update: ')
        if key_read == '': break
        value_read = input('Enter the value for the key: ')

        key_value_read.append(
            interface_pb2.KeyValueRequest(key=key_read, val=value_read)
        )
        print()

    with grpc.insecure_channel(f'localhost:{port}') as channel:
        stub = interface_pb2_grpc.KeyValueStoreStub(channel)

        try:
            responses = stub.PutAll(iter(key_value_read))

            print()
            print('Response:')
            for response in responses:
                if response.ver <= 0:
                    print(f'The key ({response.key}) is out of range or does not exist')
                else:
                    print(response)

            print()
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


def get_range(port: int = 50051):
    from_key_read = input('Type the initial key that you want to query: ')
    print('If you want the latest version, dont put any version')
    from_version_read = input('Enter initial version to search: ')

    print()

    to_key_read = input('Type the end key that you want to query: ')
    print('If you want the latest version, dont put any version')
    to_version_read = input('Enter end version to search: ')

    print()

    if from_version_read != '':
        try:
            from_version_read = int(from_version_read)
        except ValueError:
            from_version_read = -1
    else:
        from_version_read = -1

    if to_version_read != '':
        try:
            to_version_read = int(to_version_read)
        except ValueError:
            to_version_read = -1
    else:
        to_version_read = -1

    with grpc.insecure_channel(f'localhost:{port}') as channel:
        stub = interface_pb2_grpc.KeyValueStoreStub(channel)

        try:
            responses = stub.GetRange(interface_pb2.KeyRange(
                from_key=interface_pb2.KeyRequest(key=from_key_read, ver=from_version_read),
                to_key=interface_pb2.KeyRequest(key=to_key_read, ver=to_version_read)
            ))

            print()
            print('Response:')
            for response in responses:
                print(response)
        except grpc.RpcError as e:
            print(f"Error during gRPC transmission: {e}")


def get_all(port: int = 50051) -> None:
    data_read = []

    while True:
        key_read = input('Enter the key you want to fetch: ')

        if key_read == '':
            break

        version_read = input(f'Enter the version for the key ({key_read}) you want to fetch: ')

        if version_read != '':
            try:
                version_read = int(version_read)
            except ValueError:
                version_read = -1
        else:
            version_read = -1

        data_read.append(
            interface_pb2.KeyRequest(key=key_read, ver=version_read)
        )

        print()

    with grpc.insecure_channel(f'localhost:{port}') as channel:
        stub = interface_pb2_grpc.KeyValueStoreStub(channel)

        try:
            responses = stub.GetAll(iter(data_read))

            print()
            print('Response:')
            for response in responses:
                if response.ver <= 0:
                    print(f'The key ({response.key}) are out of range or do not exist')
                else:
                    print(response)

                print()
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

def delete_all(port: int = 50051) -> None:
    keys_read = []

    while True:
        key_read = input('Input key to delete: ')
        if key_read == '': break

        keys_read.append(
            interface_pb2.KeyValueRequest(key=key_read)
        )
        print()

    with grpc.insecure_channel(f'localhost:{port}') as channel:
        stub = interface_pb2_grpc.KeyValueStoreStub(channel)

        try:
            responses = stub.DelAll(iter(keys_read))

            print()
            print('Response:')
            for response in responses:
                if response.ver <= 0:
                    print('This key has already been removed')
                else:
                    print(response)
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


def options() -> None:
    print()
    print('###############')
    print('1 - [Put] Update/insert entered value and key')
    print('2 - [PutAll] Update/insert list of values and keys')
    print('3 - [Get] Returns value by key and version')
    print('4 - [GetRange] Returns values in the range between the two keys')
    print('5 - [GetAll] Returns values for keyset')
    print('6 - [Del] Removes all values associated with the key')
    print('7 - [DelRange] Removes values in the range between the two keys')
    print('8 - [DelAll] Removes all values associated with a list of keys')
    print('9 - [Trim] Removes all values associated with the key, except the latest version')
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
                put_all()
            case 3:
                get()
            case 4:
                get_range()
            case 5:
                get_all()
            case 6:
                delete()
            case 7:
                delete_range()
            case 8:
                delete_all()
            case 9:
                trim()
            case _:
                print('This option is not valid!')
