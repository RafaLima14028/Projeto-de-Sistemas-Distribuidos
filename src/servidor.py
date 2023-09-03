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

        responses = []

        self.__dictionary.insertAndUpdate('A', 'Rafael 1')
        self.__dictionary.insertAndUpdate('B', 'Rafael 2')
        self.__dictionary.insertAndUpdate('C', 'Rafael 3')

        # print(self.__dictionary.returnDictionary())

        dict_range = self.__dictionary.getRangeByKeyVersion('A', 'C')

        print(dict_range)

        for key, values in dict_range:
            for version, value in values:
                yield interface_pb2.KeyValueVersionReply(
                    key=key,
                    val=value,
                    ver=version
                )

                # responses.append(
                #     interface_pb2.KeyValueVersionReply(
                #         key=key,
                #         val=value,
                #         ver=version
                #     )
                # )

        # responses = iter(responses)
        #
        # return responses

    def GetAll(self, request_iterator, context):
        pass

    def Put(self, request, context):
        key = request.key
        value = request.val

        key_returned, value_returned, old_version_returned, new_version_returned = \
            self.__dictionary.insertAndUpdate(key, value)

        response = interface_pb2.PutReply(
            key=key_returned,
            old_val='',
            old_ver=old_version_returned,
            ver=new_version_returned
        )

        return response

    def PutAll(self, request_iterator, context):
        # Implement your PutAll logic here
        pass

    def Del(self, request, context):
        key = request.key
        version = request.ver

        response = interface_pb2.KeyValueVersionReply(
            key='Rafael',
            val='2',
            ver=3
        )

        return response

    def DelRange(self, request, context):
        # Implement your DelRange logic here
        pass

    def DelAll(self, request_iterator, context):
        # Implement your DelAll logic here
        pass

    def Trim(self, request, context):
        key = request.key

        response = interface_pb2.KeyValueVersionReply(
            key='Rafael',
            val='2',
            ver=4
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
