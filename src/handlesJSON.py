import json
import socket

from utils import ENCODING_AND_DECODING_TYPE


class HandlesJsonCache:
    def __init__(self):
        self.sk = socket.socket()
        self.sk.connect(('localhost', 30020))

    def Put(self, key: str, value: str) -> (str, str, int, int):
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

        return key_returned, old_value, old_version, new_version

    def Get(self, key: str, version: int = -1):
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

        return key_returned, value_returned, version_returned


class HandlesJsonDatabase:
    ...
