import grpc
import interface_pb2  # Import the generated protobuf Python code
import interface_pb2_grpc  # Import the generated gRPC stubs
from concurrent import futures
from manipulateDictionary import ManipulateDictionary


class KeyValueStoreServicer(interface_pb2_grpc.KeyValueStoreServicer):
    __dictionary = None

    def __init__(self):
        self.__dictionary = ManipulateDictionary()

    def Get(self, request, context):
        key = request.key
        version = request.ver

        if key == '':
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details('Unable to pass an empty key')
            raise grpc.RpcError

        key_returned, value_returned, version_returned = (
            self.__dictionary.getByKeyVersion(key=key, version=version))

        try:
            response = interface_pb2.KeyValueVersionReply(
                key=key_returned,
                val=value_returned,
                ver=version_returned
            )

            return response
        except grpc.RpcError as e:
            context.set_code(e.code())
            context.set_details(str(e.details()))
            raise grpc.RpcError

    def GetRange(self, request, context):
        from_key = request.from_key.key
        from_version = request.from_key.ver

        to_key = request.to_key.key
        to_version = request.to_key.ver

        if from_key == '' or to_key == '':
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details('Unable to pass an empty key')
            raise grpc.RpcError

        dict_range = self.__dictionary.getRangeByKeyVersion(
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

            if key == '':
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details('Unable to pass an empty key')
                raise grpc.RpcError

            if version == '':
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details('Unable to pass an empty value')
                raise grpc.RpcError

            keys.append(key)
            versions.append(version)

        try:
            for key, version in zip(keys, versions):
                version_returned, value_returned = self.__dictionary.getAllInRange(key, version)

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

        if key == '':
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details('Unable to pass an empty key')
            raise grpc.RpcError

        if value == '':
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details('Unable to pass an empty value')
            raise grpc.RpcError

        key_returned, old_value_returned, old_version_returned, new_version_returned = \
            self.__dictionary.insertAndUpdate(key, value)

        try:
            if old_version_returned <= 0:
                response = interface_pb2.PutReply(
                    key=key_returned,
                    ver=new_version_returned
                )
            else:
                response = interface_pb2.PutReply(
                    key=key_returned,
                    old_val=old_value_returned,
                    old_ver=old_version_returned,
                    ver=new_version_returned
                )

            return response
        except grpc.RpcError as e:
            context.set_code(e.code())
            context.set_details(str(e.details()))
            raise grpc.RpcError

    def PutAll(self, request_iterator, context):
        keys, values, response = [], [], []

        for request in request_iterator:
            key = request.key
            value = request.val

            if key == '':
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details('Unable to pass an empty key')
                raise grpc.RpcError

            if value == '':
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details('Unable to pass an empty value')
                raise grpc.RpcError

            keys.append(key)
            values.append(value)

        try:
            for key, value in zip(keys, values):
                key_returned, old_value_returned, old_version_returned, new_version_returned = \
                    self.__dictionary.insertAndUpdate(key, value)

                if old_value_returned == '':
                    response.append(interface_pb2.PutReply(
                        key=key_returned,
                        ver=new_version_returned
                    ))
                else:
                    response.append(interface_pb2.PutReply(
                        key=key_returned,
                        old_val=old_value_returned,
                        old_ver=old_version_returned,
                        ver=new_version_returned
                    ))

            return iter(response)
        except grpc.RpcError as e:
            context.set_code(e.code())
            context.set_details(str(e.details()))
            raise grpc.RpcError

    def Del(self, request, context):
        key = request.key

        if key == '':
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details('Unable to pass an empty key')
            raise grpc.RpcError

        key_returned, last_value, last_version = self.__dictionary.delete(key)

        try:
            response = interface_pb2.KeyValueVersionReply(
                key=key,
                val=last_value,
                ver=last_version
            )

            return response
        except grpc.RpcError as e:
            context.set_code(e.code())
            context.set_details(str(e.details()))
            raise grpc.RpcError

    def DelRange(self, request, context):
        from_key = request.from_key.key
        to_key = request.to_key.key

        if from_key == '' or to_key == '':
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details('Unable to pass an empty key')
            raise grpc.RpcError

        dict_range = self.__dictionary.delRange(from_key, to_key)

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

            if key == '':
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details('Unable to pass an empty key')
                raise grpc.RpcError

            keys.append(key)

        try:
            for key in keys:
                for data in self.__dictionary.delAll(key):
                    response = interface_pb2.KeyValueVersionReply(
                        key=key,
                        val=data[0],
                        ver=data[1]
                    )

                    yield response
        except grpc.RpcError as e:
            context.set_code(e.code())
            context.set_details(str(e.details()))
            raise grpc.RpcError

    def Trim(self, request, context):
        key = request.key

        if key == '':
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details('Unable to pass an empty key')
            raise grpc.RpcError

        key_returned, last_value, last_version = self.__dictionary.trim(key)

        try:
            response = interface_pb2.KeyValueVersionReply(
                key=key_returned,
                val=last_value,
                ver=last_version
            )

            return response
        except grpc.RpcError as e:
            context.set_code(e.code())
            context.set_details(str(e.details()))
            raise grpc.RpcError


def serve(port: int = 50051):
    try:
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        interface_pb2_grpc.add_KeyValueStoreServicer_to_server(KeyValueStoreServicer(), server)
        server.add_insecure_port(f'localhost:{port}')  # Set the server port
    except grpc.RpcError as e:
        print(f'Error during gRPC startup: {e}')

    server.start()

    print('Server listening...')

    server.wait_for_termination()


if __name__ == '__main__':
    serve()
