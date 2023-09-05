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

        response_dict = self.__dictionary.getByKeyVersion(key=key, version=version)

        if response_dict == (None, None):
            response = interface_pb2.KeyValueVersionReply(
                key=key,
                val='',
                ver=-1
            )
        else:
            response = interface_pb2.KeyValueVersionReply(
                key=key,
                val=response_dict[0],
                ver=response_dict[1]
            )

        return response

    def GetRange(self, request, context):
        from_key = request.from_key.key
        from_version = request.from_key.ver
        to_key = request.to_key.key
        to_version = request.to_key.ver

        dict_range = self.__dictionary.getRangeByKeyVersion(
            from_key, to_key,
            from_version, to_version
        )

        for key, values in dict_range.items():
            for version, value in values:
                response = interface_pb2.KeyValueVersionReply(
                    key=key,
                    val=value,
                    ver=version
                )

                yield response

    def GetAll(self, request_iterator, context):
        for request in request_iterator:
            key = request.key
            version = request.ver

            list_range = self.__dictionary.getAllInRange(key, version)

            for list_data in list_range:
                for tupla in list_data:
                    response = interface_pb2.KeyValueVersionReply(
                        key=key,
                        val=tupla[1],
                        ver=tupla[0]
                    )

                    yield response

    def Put(self, request, context):
        key = request.key
        value = request.val

        key_returned, old_value_returned, old_version_returned, new_version_returned = \
            self.__dictionary.insertAndUpdate(key, value)

        if old_value_returned == '':
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

    def PutAll(self, request_iterator, context):
        # Implement your PutAll logic here
        pass

    def Del(self, request, context):
        key = request.key

        key_returned, last_value, last_version = self.__dictionary.delete(key)

        response = interface_pb2.KeyValueVersionReply(
            key=key,
            val=last_value,
            ver=last_version
        )

        return response

    def DelRange(self, request, context):
        from_key = request.from_key.key
        to_key = request.to_key.key

        dict_range = self.__dictionary.delRange(from_key, to_key)

        for key, values in dict_range.items():
            for version, value in values:
                response = interface_pb2.KeyValueVersionReply(
                    key=key,
                    val=value,
                    ver=version
                )

                yield response

    def DelAll(self, request_iterator, context):
        for request in request_iterator:
            key = request.key

            for data in self.__dictionary.delAll(key):
                response = interface_pb2.KeyValueVersionReply(
                    key=key,
                    val=data[0],
                    ver=data[1]
                )

                yield response

    def Trim(self, request, context):
        key = request.key

        key_returned, last_value, last_version = self.__dictionary.trim(key)

        response = interface_pb2.KeyValueVersionReply(
            key=key_returned,
            val=last_value,
            ver=last_version
        )

        return response


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    interface_pb2_grpc.add_KeyValueStoreServicer_to_server(KeyValueStoreServicer(), server)
    server.add_insecure_port('localhost:50051')  # Set the server port
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    serve()
