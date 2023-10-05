import sys
from concurrent import futures
import grpc
import re

from src.interface import interface_pb2, interface_pb2_grpc
from db import Database


def check_string(text: str) -> bool:
    pattern = r"^(?!\s*$)(?!.*[,])"

    if re.match(pattern, text):
        return False
    else:
        return True  # There is an error


class KeyValueStoreServicer(interface_pb2_grpc.KeyValueStoreServicer):
    def __init__(self):
        self.database = Database('my_db')
        # self.dictionary = ManipulateDictionary()

    def Get(self, request, context):
        key = request.key
        version = request.ver

        if check_string(key):
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details('You entered some wrong value')
            raise grpc.RpcError

        # key_returned, value_returned, version_returned = (
        #     self.dictionary.getByKeyVersion(key=key, version=version)
        # )

        key_returned, value_returned, version_returned = self.database.get(key, version)

        try:
            return interface_pb2.KeyValueVersionReply(
                key=key_returned,
                val=value_returned,
                ver=version_returned
            )
        except grpc.RpcError as e:
            context.set_code(e.code())
            context.set_details(str(e.details()))
            raise grpc.RpcError

    def GetRange(self, request, context):
        from_key = request.fr.key
        from_version = request.fr.ver

        to_key = request.to.key
        to_version = request.to.ver

        if check_string(from_key):
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details('You entered some wrong value')
            raise grpc.RpcError

        if check_string(to_key):
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details('You entered some wrong value')
            raise grpc.RpcError

        dict_range = self.dictionary.getRangeByKeyVersion(
            from_key, to_key,
            from_version, to_version
        )

        try:
            for key, values in dict_range.items():
                for version, value in values:
                    response = interface_pb2.KeyValueVersionReply(
                        key=key,
                        val=value,
                        ver=version
                    )

                    yield response
        except grpc.RpcError as e:
            context.set_code(e.code())
            context.set_details(str(e.details()))
            raise grpc.RpcError

    def GetAll(self, request_iterator, context):
        keys, versions, response = [], [], []

        for request in request_iterator:
            key = request.key
            version = request.ver

            if check_string(key):
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details('You entered some wrong value')
                raise grpc.RpcError

            keys.append(key)
            versions.append(version)

        try:
            for key, version in zip(keys, versions):
                version_returned, value_returned = self.dictionary.getAllInRange(key, version)

                response = interface_pb2.KeyValueVersionReply(
                    key=key,
                    val=value_returned,
                    ver=version_returned
                )

                yield response
        except grpc.RpcError as e:
            context.set_code(e.code())
            context.set_details(str(e.details()))
            raise grpc.RpcError

    def Put(self, request, context):
        key = request.key
        value = request.val

        if check_string(key):
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details('You entered some wrong value')
            raise grpc.RpcError

        if check_string(value):
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details('You entered some wrong value')
            raise grpc.RpcError

        # key_returned, old_value_returned, old_version_returned, new_version_returned = \
        #     self.dictionary.insertAndUpdate(key, value)

        key_returned, old_value_returned, old_version_returned, new_version_returned = \
            self.database.put(key, value)

        try:
            return interface_pb2.PutReply(
                key=key_returned,
                old_val=old_value_returned,
                old_ver=old_version_returned,
                ver=new_version_returned
            )
        except grpc.RpcError as e:
            context.set_code(e.code())
            context.set_details(str(e.details()))
            raise grpc.RpcError

    def PutAll(self, request_iterator, context):
        keys, values, response = [], [], []

        for request in request_iterator:
            key = request.key
            value = request.val

            if check_string(key):
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details('You entered some wrong value')
                raise grpc.RpcError

            if check_string(value):
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details('You entered some wrong value')
                raise grpc.RpcError

            keys.append(key)
            values.append(value)

        try:
            for key, value in zip(keys, values):
                key_returned, old_value_returned, old_version_returned, new_version_returned = \
                    self.dictionary.insertAndUpdate(key, value)

                response.append(
                    interface_pb2.PutReply(
                        key=key_returned,
                        old_val=old_value_returned,
                        old_ver=old_version_returned,
                        ver=new_version_returned
                    )
                )

            return iter(response)
        except grpc.RpcError as e:
            context.set_code(e.code())
            context.set_details(str(e.details()))
            raise grpc.RpcError

    def Del(self, request, context):
        key = request.key

        if check_string(key):
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details('You entered some wrong value')
            raise grpc.RpcError

        # key_returned, last_value, last_version = self.dictionary.delete(key)
        key_returned, last_value, last_version = self.database.delete(key)

        try:
            return interface_pb2.KeyValueVersionReply(
                key=key,
                val=last_value,
                ver=last_version
            )
        except grpc.RpcError as e:
            context.set_code(e.code())
            context.set_details(str(e.details()))
            raise grpc.RpcError

    def DelRange(self, request, context):
        from_key = request.fr.key
        to_key = request.to.key

        if check_string(from_key):
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details('You entered some wrong value')
            raise grpc.RpcError

        if check_string(to_key):
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details('You entered some wrong value')
            raise grpc.RpcError

        dict_range = self.dictionary.delRange(from_key, to_key)

        try:
            for key, values in dict_range.items():
                for version, value in values:
                    response = interface_pb2.KeyValueVersionReply(
                        key=key,
                        val=value,
                        ver=version
                    )

                    yield response
        except grpc.RpcError as e:
            context.set_code(e.code())
            context.set_details(str(e.details()))
            raise grpc.RpcError

    def DelAll(self, request_iterator, context):
        keys = []

        for request in request_iterator:
            key = request.key

            if check_string(key):
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details('You entered some wrong value')
                raise grpc.RpcError

            keys.append(key)

        try:
            for key in keys:
                key_returned, value_returned, version_returned = self.dictionary.delAll(key)

                response = interface_pb2.KeyValueVersionReply(
                    key=key_returned,
                    val=value_returned,
                    ver=version_returned
                )

                yield response
        except grpc.RpcError as e:
            context.set_code(e.code())
            context.set_details(str(e.details()))
            raise grpc.RpcError

    def Trim(self, request, context):
        key = request.key

        if check_string(key):
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details('You entered some wrong value')
            raise grpc.RpcError

        # key_returned, last_value, last_version = self.dictionary.trim(key)
        key_returned, last_value, last_version = self.database.trim(key)

        try:
            return interface_pb2.KeyValueVersionReply(
                key=key_returned,
                val=last_value,
                ver=last_version
            )
        except grpc.RpcError as e:
            context.set_code(e.code())
            context.set_details(str(e.details()))
            raise grpc.RpcError


def serve(port: int) -> None:
    class_work = KeyValueStoreServicer()

    try:
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        interface_pb2_grpc.add_KeyValueStoreServicer_to_server(class_work, server)
        server.add_insecure_port(f'localhost:{port}')  # Set the server port
    except grpc.RpcError as e:
        print(f'Error during gRPC startup: {e}')

    server.start()

    print('Server listening on port ' + str(port) + '...')

    server.wait_for_termination()


if __name__ == '__main__':
    try:
        port = int(sys.argv[1])
    except Exception as e:
        port = 50051

    serve(port)
