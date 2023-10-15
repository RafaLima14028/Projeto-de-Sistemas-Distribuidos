import sys
from concurrent import futures
import grpc
import socket
import json

import project.interface_pb2_grpc as interface_pb2_grpc
import project.interface_pb2 as interface_pb2
from manipulateDictionary import ManipulateDictionary
from utils import check_string, ENCODING_AND_DECODING_TYPE


class KeyValueStoreServicer(interface_pb2_grpc.KeyValueStoreServicer):
    def __init__(self):
        self.sk = socket.socket()
        self.sk.connect(('localhost', 30020))
        # self.dictionary = ManipulateDictionary()

    def Get(self, request, context):
        key = request.key
        version = request.ver

        if check_string(key):
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details('You entered some wrong value')
            raise grpc.RpcError

        msg = json.dumps(
            {
                'function': 'get',
                'key': key,
                'value': '',
                'version': version
            }
        )

        self.sk.send(msg.encode(ENCODING_AND_DECODING_TYPE))
        resp = self.sk.recv(16480)
        resp = resp.decode(ENCODING_AND_DECODING_TYPE)

        data = json.loads(resp)

        key_returned = data['key']
        value_returned = data['value_returned']
        version_returned = data['version_returned']

        # key_returned, value_returned, version_returned = (
        #     self.dictionary.getByKeyVersion(key=key, version=version)
        # )

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

        # dict_range = self.dictionary.getRangeByKeyVersion(
        #     from_key, to_key,
        #     from_version, to_version
        # )

        msg = json.dumps(
            {
                'function': 'getRange',
                'start_key': from_key,
                'end_key': to_key,
                'start_version': from_version,
                'end_version': to_version
            }
        )

        self.sk.send(msg.encode(ENCODING_AND_DECODING_TYPE))
        resp = self.sk.recv(16480)
        resp = resp.decode(ENCODING_AND_DECODING_TYPE)

        data = json.loads(resp)

        try:
            for key, values in data.items():
                for version_value, value_value in values:
                    response = interface_pb2.KeyValueVersionReply(
                        key=key,
                        val=value_value,
                        ver=version_value
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
                # version_returned, value_returned = self.dictionary.getAllInRange(key, version)

                msg = json.dumps(
                    {
                        'function': 'getAll',
                        'key': key,
                        'value': '',
                        'version': version
                    }
                )

                self.sk.send(msg.encode(ENCODING_AND_DECODING_TYPE))
                resp = self.sk.recv(16480)
                resp = resp.decode(ENCODING_AND_DECODING_TYPE)

                data = json.loads(resp)

                key_returned = data['key']
                value_returned = data['value_returned']
                version_returned = data['version_returned']

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

        # key_returned, old_value_returned, old_version_returned, new_version_returned = \
        #     self.dictionary.insertAndUpdate(key, value)

        msg = json.dumps(
            {
                'function': 'put',
                'key': key,
                'value': value,
                'version': -1
            }
        )

        self.sk.send(msg.encode(ENCODING_AND_DECODING_TYPE))
        resp = self.sk.recv(16480)
        resp = resp.decode(ENCODING_AND_DECODING_TYPE)

        data = json.loads(resp)

        key_returned = data['key']
        old_value = data['old_value']
        old_version = data['old_version']
        new_version = data['new_version']

        try:
            return interface_pb2.PutReply(
                key=key_returned,
                old_val=old_value,
                old_ver=old_version,
                ver=new_version
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
                # key_returned, old_value_returned, old_version_returned, new_version_returned = \
                #     self.dictionary.insertAndUpdate(key, value)

                msg = json.dumps(
                    {
                        'function': 'putAll',
                        'key': key,
                        'value': value,
                        'version': -1
                    }
                )

                self.sk.send(msg.encode(ENCODING_AND_DECODING_TYPE))
                resp = self.sk.recv(16480)
                resp = resp.decode(ENCODING_AND_DECODING_TYPE)

                data = json.loads(resp)

                key_returned = data['key']
                old_value = data['old_value']
                old_version = data['old_version']
                new_version = data['new_version']

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

        # key_returned, last_value, last_version = self.dictionary.delete(key)

        msg = json.dumps(
            {
                'function': 'del',
                'key': key,
                'value': '',
                'version': -1
            }
        )

        self.sk.send(msg.encode(ENCODING_AND_DECODING_TYPE))
        resp = self.sk.recv(16480)
        resp = resp.decode(ENCODING_AND_DECODING_TYPE)

        data = json.loads(resp)

        key_returned = data['key']
        last_value = data['last_value']
        last_version = data['last_version']

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

        # dict_range = self.dictionary.delRange(from_key, to_key)

        msg = json.dumps(
            {
                'function': 'delRange',
                'start_key': from_key,
                'end_key': to_key
            }
        )

        self.sk.send(msg.encode(ENCODING_AND_DECODING_TYPE))
        resp = self.sk.recv(16480)
        resp = resp.decode(ENCODING_AND_DECODING_TYPE)

        data = json.loads(resp)

        try:
            for key, values in data.items():
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
                # key_returned, value_returned, version_returned = self.dictionary.delAll(key)

                msg = json.dumps(
                    {
                        'function': 'delAll',
                        'key': key,
                        'value': '',
                        'version': -1
                    }
                )

                self.sk.send(msg.encode(ENCODING_AND_DECODING_TYPE))
                resp = self.sk.recv(16480)
                resp = resp.decode(ENCODING_AND_DECODING_TYPE)

                data = json.loads(resp)

                key_returned = data['key']
                value_returned = data['last_value']
                version_returned = data['last_version']

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

        msg = json.dumps(
            {
                'function': 'trim',
                'key': key,
                'value': '',
                'version': -1
            }
        )

        self.sk.send(msg.encode(ENCODING_AND_DECODING_TYPE))
        resp = self.sk.recv(16480)
        resp = resp.decode(ENCODING_AND_DECODING_TYPE)

        data = json.loads(resp)

        key_returned = data['key']
        last_value = data['last_value']
        new_version = data['new_version']

        try:
            return interface_pb2.KeyValueVersionReply(
                key=key_returned,
                val=last_value,
                ver=new_version
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
