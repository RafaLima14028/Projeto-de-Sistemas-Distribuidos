import sys

import grpc

import interface_pb2  # Import the generated protobuf Python code
import interface_pb2_grpc  # Import the generated gRPC stubs


def check_number(number: str) -> int:
    if number != '':
        try:
            number = int(number)
        except ValueError:
            number = -1
    else:
        number = -1

    return number


def put(port: int, key: str, value: str) -> None:
    with grpc.insecure_channel(f'localhost:{port}') as channel:
        stub = interface_pb2_grpc.KeyValueStoreStub(channel)

        try:
            reply = stub.Put(
                interface_pb2.KeyValueRequest(key=key, val=value)
            )

            print()
            print('Response:')
            print(reply)
        except grpc.RpcError as e:
            print()

            if e.code() == grpc.StatusCode.INVALID_ARGUMENT:
                print(f'Server error: {e.details()}')
            else:
                print(f"Error during gRPC transmission: {e.details()}")


def put_all(port: int, keys_values_read: []) -> None:
    keys_values = []
    for i in range(0, len(keys_values_read)):
        keys_values.append(
            interface_pb2.KeyValueRequest(key=keys_values_read[i][0], val=keys_values_read[i][1])
        )

    with grpc.insecure_channel(f'localhost:{port}') as channel:
        stub = interface_pb2_grpc.KeyValueStoreStub(channel)

        try:
            responses = stub.PutAll(iter(keys_values))

            print()
            print('Response:')
            for response in responses:
                if response.ver <= 0:
                    print(f'The key ({response.key}) is out of range or does not exist')
                else:
                    print(response)

            print()
        except grpc.RpcError as e:
            print()

            if e.code() == grpc.StatusCode.INVALID_ARGUMENT:
                print(f'Server error: {e.details()}')
            else:
                print(f"Error during gRPC transmission: {e.details()}")


def get(port: int, key: str, version: int) -> None:
    with grpc.insecure_channel(f'localhost:{port}') as channel:
        stub = interface_pb2_grpc.KeyValueStoreStub(channel)

        try:
            reply = stub.Get(interface_pb2.KeyRequest(key=key, ver=version))

            if reply.ver <= 0:
                print()
                print('Key or version is not present')
            else:
                print()
                print('Response:')
                print(reply)
        except grpc.RpcError as e:
            print()

            if e.code() == grpc.StatusCode.INVALID_ARGUMENT:
                print(f'Server error: {e.details()}')
            else:
                print(f"Error during gRPC transmission: {e.details()}")


def get_range(port: int, from_key: str, to_key: str, from_version: int, to_version: int):
    with grpc.insecure_channel(f'localhost:{port}') as channel:
        stub = interface_pb2_grpc.KeyValueStoreStub(channel)

        try:
            responses = stub.GetRange(
                interface_pb2.KeyRange(
                    fr=interface_pb2.KeyRequest(key=from_key, ver=from_version),
                    to=interface_pb2.KeyRequest(key=to_key, ver=to_version)
                )
            )

            print()
            print('Response:')
            for response in responses:
                print(response)
        except grpc.RpcError as e:
            print()

            if e.code() == grpc.StatusCode.INVALID_ARGUMENT:
                print(f'Server error: {e.details()}')
            else:
                print(f"Error during gRPC transmission: {e.details()}")


def get_all(port: int, keys_versions_read: []) -> None:
    keys_versions = []
    for i in range(0, len(keys_versions_read)):
        keys_versions.append(
            interface_pb2.KeyRequest(key=keys_versions_read[i][0], ver=keys_versions_read[i][1])
        )

    with grpc.insecure_channel(f'localhost:{port}') as channel:
        stub = interface_pb2_grpc.KeyValueStoreStub(channel)

        try:
            responses = stub.GetAll(iter(keys_versions))

            print()
            print('Response:')
            for response in responses:
                if response.ver <= 0:
                    print(f'The key ({response.key}) are out of range or do not exist')
                else:
                    print(response)

                print()
        except grpc.RpcError as e:
            print()

            if e.code() == grpc.StatusCode.INVALID_ARGUMENT:
                print(f'Server error: {e.details()}')
            else:
                print(f"Error during gRPC transmission: {e.details()}")


def delete(port: int, key: str) -> None:
    with grpc.insecure_channel(f'localhost:{port}') as channel:
        stub = interface_pb2_grpc.KeyValueStoreStub(channel)

        try:
            reply = stub.Del(
                interface_pb2.KeyValueRequest(key=key)
            )

            if reply.ver <= 0:
                print()
                print('This key has already been removed')
            else:
                print()
                print('Response:')
                print(reply)
        except grpc.RpcError as e:
            print()

            if e.code() == grpc.StatusCode.INVALID_ARGUMENT:
                print(f'Server error: {e.details()}')
            else:
                print(f"Error during gRPC transmission: {e.details()}")


def delete_range(port: int, from_key: str, to_key: str) -> None:
    with grpc.insecure_channel(f'localhost:{port}') as channel:
        stub = interface_pb2_grpc.KeyValueStoreStub(channel)

        try:
            replies = stub.DelRange(
                interface_pb2.KeyRange(
                    fr=interface_pb2.KeyRequest(key=from_key),
                    to=interface_pb2.KeyRequest(key=to_key)
                )
            )

            print()
            print('Response:')
            for reply in replies:
                print(reply)
        except grpc.RpcError as e:
            print()

            if e.code() == grpc.StatusCode.INVALID_ARGUMENT:
                print(f'Server error: {e.details()}')
            else:
                print(f"Error during gRPC transmission: {e.details()}")


def delete_all(port: int, keys_read: []) -> None:
    keys = []
    for i in range(0, len(keys_read)):
        keys.append(
            interface_pb2.KeyValueRequest(key=keys_read[i])
        )

    with grpc.insecure_channel(f'localhost:{port}') as channel:
        stub = interface_pb2_grpc.KeyValueStoreStub(channel)

        try:
            responses = stub.DelAll(iter(keys))

            print()
            print('Response:')
            for response in responses:
                if response.ver <= 0:
                    print('This key has already been removed or not exists')
                else:
                    print(response)
        except grpc.RpcError as e:
            print()

            if e.code() == grpc.StatusCode.INVALID_ARGUMENT:
                print(f'Server error: {e.details()}')
            else:
                print(f"Error during gRPC transmission: {e.details()}")


def trim(port: int, key: str) -> None:
    with grpc.insecure_channel(f'localhost:{port}') as channel:
        stub = interface_pb2_grpc.KeyValueStoreStub(channel)

        try:
            reply = stub.Trim(
                interface_pb2.KeyRequest(key=key)
            )

            if reply.ver <= 0:
                print()
                print('This key has already been removed')
            else:
                print()
                print('Response:')
                print(reply)
        except grpc.RpcError as e:
            print()

            if e.code() == grpc.StatusCode.INVALID_ARGUMENT:
                print(f'Server error: {e.details()}')
            else:
                print(f"Error during gRPC transmission: {e.details()}")


def menu_options() -> None:
    print()
    print('###############')
    print('1 - [Put] Update/insert entered value and key')
    print('2 - [PutAll] Update/insert list of values and keys')
    print('3 - [Get] Returns value by key and version')
    print('4 - [GetRange] Returns values in the range between the two keys')
    print('5 - [GetAll] Returns all values for a list of keys')
    print('6 - [Del] Removes all values associated with the key')
    print('7 - [DelRange] Removes values in the range between the two keys')
    print('8 - [DelAll] Removes all values associated with a list of keys')
    print('9 - [Trim] Removes all values associated with the key, except the latest version')
    print('###############')
    print()


def menu() -> None:
    while True:
        menu_options()
        option = input('Please enter a valid option: ')

        match option:
            case '1':  # [Put]
                key_read = input('Enter key to insert/update: ')
                value_read = input('Enter the value for the key: ')
                put(port, key_read, value_read)

            case '2':  # [PutAll]
                key_value_read = []
                while True:
                    key_read = input('Enter key to insert/update: ')
                    if key_read == '':
                        break
                    value_read = input('Enter the value for the key: ')
                    if value_read == '':
                        print('Invalid value\n')
                        continue
                    key_value_read.append((key_read, value_read))
                put_all(port, key_value_read)

            case '3':  # [Get]
                key_read = input('Enter the key for the search: ')
                print('If you want the latest version, dont put any version')
                version_read = input('Enter version to search: ')
                version_read = check_number(version_read)
                get(port, key_read, version_read)

            case '4':  # [GetRange]
                from_key_read = input('Type the initial key that you want to query: ')
                print('If you want the latest version, dont put any version')
                from_version_read = input('Enter initial version to search: ')
                print()
                to_key_read = input('Type the end key that you want to query: ')
                print('If you want the latest version, dont put any version')
                to_version_read = input('Enter end version to search: ')
                print()
                from_version_read = check_number(from_version_read)
                to_version_read = check_number(to_version_read)
                get_range(port, from_key_read, to_key_read, from_version_read, to_version_read)

            case '5':  # [GetAll]
                keys_versions_read = []
                while True:
                    key_read = input('Enter the key you want to fetch: ')
                    if key_read == '':
                        break
                    version_read = input(f'Enter the version for the key ({key_read}) you want to fetch: ')
                    version_read = check_number(version_read)
                    keys_versions_read.append((key_read, version_read))
                get_all(port, keys_versions_read)

            case '6':  # [Delete]
                key_read = input('Enter the key you want to remove: ')
                delete(port, key_read)

            case '7':  # [DeleteRange]
                from_key_read = input('Enter the initial key you want to remove: ')
                end_key_read = input('Enter the end key you want to remove: ')
                delete_range(port, from_key_read, end_key_read)

            case '8':  # [DeleteAll]
                keys_read = []
                while True:
                    key_read = input('Input key to delete: ')
                    if key_read == '':
                        break
                    keys_read.append(key_read)
                delete_all(port, keys_read)

            case '9':  # [Trim]
                key_read = input('Enter the key you want to remove: ')
                trim(port, key_read)

            case _:
                print('This option is not valid!')


if __name__ == '__main__':
    try:
        port = int(sys.argv[1])
    except Exception as e:
        port = 50051

    menu()
