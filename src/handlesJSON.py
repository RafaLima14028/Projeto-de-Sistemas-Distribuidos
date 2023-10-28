import json
import socket

from utils import ENCODING_AND_DECODING_TYPE


class HandlesJsonCache:
    def __init__(self):
        self.sk = socket.socket()
        self.sk.connect(('localhost', 30020))

    def Get(self, key: str, version: int = -1) -> (str, str, int):
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

    def GetRange(self, from_key: str, to_key: str, from_version: int = -1, to_version: int = -1) -> dict:
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

        return data

    def GetAll(self, key: str, version: int = -1) -> (str, str, int):
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

        return key_returned, value_returned, version_returned

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

    def PutAll(self, key: str, value: str) -> (str, str, int, int):
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

        return key_returned, old_value, old_version, new_version

    def Del(self, key: str) -> (str, str, int):
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

        return key_returned, last_value, last_version

    def DelRange(self, from_key: str, to_key: str) -> dict:
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

        return data

    def DelAll(self, key: str) -> (str, str, int):
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

        return key_returned, value_returned, version_returned

    def Trim(self, key: str) -> (str, str, int):
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

        return key_returned, last_value, new_version
