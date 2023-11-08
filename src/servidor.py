import sys
from concurrent import futures
import grpc

from src.proto import interface_pb2_grpc, interface_pb2
from src.manipulateDictionary import ManipulateDictionary
from src.utils import check_string


class KeyValueStoreServicer(interface_pb2_grpc.KeyValueStoreServicer):
    def __init__(self):
        self.__cache = ManipulateDictionary()

    def Get(self, request, context):
        key = request.key
        version = request.ver

        if check_string(key):
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details('You entered some wrong value')
            raise grpc.RpcError

        key_returned, value_returned, version_returned = (
            self.__cache.getByKeyVersion(key, version)
        )

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

        data = self.__cache.getRangeByKeyVersion(
            from_key, to_key,
            from_version, to_version
        )

        try:
            for key, values in data.items():
                yield interface_pb2.KeyValueVersionReply(
                    key=key,
                    val=values[1],
                    ver=values[0]
                )
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
                key_returned, value_returned, version_returned = self.__cache.getByKeyVersion(key, version)

                if key_returned == '' and value_returned == '' and version_returned <= 0:
                    response = interface_pb2.KeyValueVersionReply(
                        key=key,
                        val='',
                        ver=-1
                    )
                else:
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

        key_returned, old_value_returned, old_version_returned, new_version_returned = \
            self.__cache.insertAndUpdate(key, value)

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
                key_returned, old_value, old_version, new_version = self.__cache.insertAndUpdate(key, value)

                response.append(
                    interface_pb2.PutReply(
                        key=key_returned,
                        old_val=old_value,
                        old_ver=old_version,
                        ver=new_version
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

        key_returned, last_value, last_version = self.__cache.delete(key)

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

        data = self.__cache.delRange(from_key, to_key)

        try:
            for key, values in data.items():
                yield interface_pb2.KeyValueVersionReply(
                    key=key,
                    val=values[1],
                    ver=values[0]
                )
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
                key_returned, value_returned, version_returned = self.__cache.delete(key)

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

        key_returned, last_value, last_version = self.__cache.trim(key)

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


def serve(port: int = 50051) -> None:
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
