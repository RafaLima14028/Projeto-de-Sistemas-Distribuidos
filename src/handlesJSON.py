import json
import socket

from src.utils import ENCODING_AND_DECODING_TYPE, SERVER_DB_ADDRESS, SERVER_DB_SOCKET_PORT


class HandlesJsonCache:
    def __init__(self):
        self.sk = socket.socket()

        self.__connected = False
        self.__socket_port = -1

        self.__make_new_connection()

        print(f'The cache is connected with the replica: {SERVER_DB_ADDRESS}:{self.__socket_port}')

    def __make_new_connection(self) -> int:
        """"
        Return Messages:
             1: Connection Successfully
            -1: You are already connected
            -2: Tried a new connection, but it was not possible
        """

        if not self.__is_connected():
            for i in range(3):
                if not self.__connected:
                    port = SERVER_DB_SOCKET_PORT + i

                    try:
                        self.sk.connect((SERVER_DB_ADDRESS, port))
                        self.__connected = True
                        self.__socket_port = port
                        break
                    except socket.error:
                        pass

            if not self.__connected and self.__socket_port > 0:
                return 1
            else:
                return -2
        else:
            return -1

    def __is_connected(self) -> bool:
        if self.__connected and self.__socket_port > 0:
            return True
        else:
            return False

    def Get(self, key: str, version: int = -1) -> (str, str, int):
        if not self.__is_connected():
            resp_conn = self.__make_new_connection()

            if resp_conn == -2:
                raise Exception(
                    f'Error in handlesJSON Get: There is no replica online'
                )

        msg = json.dumps(
            {
                'function': 'get',
                'key': key,
                'value': '',
                'version': version
            }
        )

        self.sk.send(msg.encode(ENCODING_AND_DECODING_TYPE))

        resp = None

        try:
            resp = self.sk.recv(16480)
        except ConnectionResetError:
            resp_new_connection = self.__make_new_connection()

            if resp_new_connection < 0:
                self.sk.close()

                raise Exception(
                    f'Error in handlesJSON in Get: The connection was '
                    f'interrupted at an unexpected time, the reconnection was attempted but failed.'
                )

            resp = self.sk.recv(16880)

        resp = resp.decode(ENCODING_AND_DECODING_TYPE)

        data = json.loads(resp)

        if 'error' in data:
            raise Exception(
                f'Error in handlesJSON Get: {str(data["error"])}'
            )

        key_returned = data['key']
        value_returned = data['value_returned']
        version_returned = data['version_returned']

        return key_returned, value_returned, version_returned

    def GetRange(self, from_key: str, to_key: str, from_version: int = -1, to_version: int = -1) -> dict:
        if not self.__is_connected():
            resp_conn = self.__make_new_connection()

            if resp_conn == -2:
                raise Exception(
                    f'Error in handlesJSON GetRange: There is no replica online'
                )

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

        resp = None

        try:
            resp = self.sk.recv(16480)
        except ConnectionResetError:
            resp_new_connection = self.__make_new_connection()

            if resp_new_connection < 0:
                self.sk.close()

                raise Exception(
                    f'Error in handlesJSON in GetRange: The connection was '
                    f'interrupted at an unexpected time, the reconnection was attempted but failed.'
                )

            resp = self.sk.recv(16880)

        resp = resp.decode(ENCODING_AND_DECODING_TYPE)

        data = json.loads(resp)

        if 'error' in data:
            raise Exception(
                f'Error in handlesJSON GetRange: {str(data["error"])}'
            )

        return data

    def GetAll(self, key: str, version: int = -1) -> (str, str, int):
        if not self.__is_connected():
            resp_conn = self.__make_new_connection()

            if resp_conn == -2:
                raise Exception(
                    f'Error in handlesJSON GetAll: There is no replica online'
                )

        msg = json.dumps(
            {
                'function': 'getAll',
                'key': key,
                'value': '',
                'version': version
            }
        )

        self.sk.send(msg.encode(ENCODING_AND_DECODING_TYPE))

        resp = None

        try:
            resp = self.sk.recv(16480)
        except ConnectionResetError:
            resp_new_connection = self.__make_new_connection()

            if resp_new_connection < 0:
                self.sk.close()

                raise Exception(
                    f'Error in handlesJSON in GetAll: The connection was '
                    f'interrupted at an unexpected time, the reconnection was attempted but failed.'
                )

            resp = self.sk.recv(16880)

        resp = resp.decode(ENCODING_AND_DECODING_TYPE)

        data = json.loads(resp)

        if 'error' in data:
            raise Exception(
                f'Error in handlesJSON GetAll: {str(data["error"])}'
            )

        key_returned = data['key']
        value_returned = data['value_returned']
        version_returned = data['version_returned']

        return key_returned, value_returned, version_returned

    def Put(self, key: str, value: str) -> (str, str, int, int):
        if not self.__is_connected():
            resp_conn = self.__make_new_connection()

            if resp_conn == -2:
                raise Exception(
                    f'Error in handlesJSON Put: There is no replica online'
                )

        msg = json.dumps(
            {
                'function': 'put',
                'key': key,
                'value': value,
                'version': -1
            }
        )

        self.sk.send(msg.encode(ENCODING_AND_DECODING_TYPE))

        resp = None

        try:
            resp = self.sk.recv(16480)
        except ConnectionResetError:
            resp_new_connection = self.__make_new_connection()

            if resp_new_connection < 0:
                self.sk.close()

                raise Exception(
                    f'Error in handlesJSON in Put: The connection was '
                    f'interrupted at an unexpected time, the reconnection was attempted but failed.'
                )

            resp = self.sk.recv(16880)

        resp = resp.decode(ENCODING_AND_DECODING_TYPE)

        data = json.loads(resp)

        if 'error' in data:
            raise Exception(
                f'Error in handlesJSON Put: {str(data["error"])}'
            )

        key_returned = data['key']
        old_value = data['old_value']
        old_version = data['old_version']
        new_version = data['new_version']

        return key_returned, old_value, old_version, new_version

    def PutAll(self, key: str, value: str) -> (str, str, int, int):
        if not self.__is_connected():
            resp_conn = self.__make_new_connection()

            if resp_conn == -2:
                raise Exception(
                    f'Error in handlesJSON PutAll: There is no replica online'
                )

        msg = json.dumps(
            {
                'function': 'putAll',
                'key': key,
                'value': value,
                'version': -1
            }
        )

        self.sk.send(msg.encode(ENCODING_AND_DECODING_TYPE))

        resp = None

        try:
            resp = self.sk.recv(16480)
        except ConnectionResetError:
            resp_new_connection = self.__make_new_connection()

            if resp_new_connection < 0:
                self.sk.close()

                raise Exception(
                    f'Error in handlesJSON in PutAll: The connection was '
                    f'interrupted at an unexpected time, the reconnection was attempted but failed.'
                )

            resp = self.sk.recv(16880)

        resp = resp.decode(ENCODING_AND_DECODING_TYPE)

        data = json.loads(resp)

        if 'error' in data:
            raise Exception(
                f'Error in handlesJSON PutAll: {str(data["error"])}'
            )

        key_returned = data['key']
        old_value = data['old_value']
        old_version = data['old_version']
        new_version = data['new_version']

        return key_returned, old_value, old_version, new_version

    def Del(self, key: str) -> (str, str, int):
        if not self.__is_connected():
            resp_conn = self.__make_new_connection()

            if resp_conn == -2:
                raise Exception(
                    f'Error in handlesJSON Del: There is no replica online'
                )

        msg = json.dumps(
            {
                'function': 'del',
                'key': key,
                'value': '',
                'version': -1
            }
        )

        self.sk.send(msg.encode(ENCODING_AND_DECODING_TYPE))

        resp = None

        try:
            resp = self.sk.recv(16480)
        except ConnectionResetError:
            resp_new_connection = self.__make_new_connection()

            if resp_new_connection < 0:
                self.sk.close()

                raise Exception(
                    f'Error in handlesJSON in Del: The connection was '
                    f'interrupted at an unexpected time, the reconnection was attempted but failed.'
                )

            resp = self.sk.recv(16880)

        resp = resp.decode(ENCODING_AND_DECODING_TYPE)

        data = json.loads(resp)

        if 'error' in data:
            raise Exception(
                f'Error in handlesJSON Del: {str(data["error"])}'
            )

        key_returned = data['key']
        last_value = data['last_value']
        last_version = data['last_version']

        return key_returned, last_value, last_version

    def DelRange(self, from_key: str, to_key: str) -> dict:
        if not self.__is_connected():
            resp_conn = self.__make_new_connection()

            if resp_conn == -2:
                raise Exception(
                    f'Error in handlesJSON DelRange: There is no replica online'
                )

        msg = json.dumps(
            {
                'function': 'delRange',
                'start_key': from_key,
                'end_key': to_key
            }
        )

        self.sk.send(msg.encode(ENCODING_AND_DECODING_TYPE))

        resp = None

        try:
            resp = self.sk.recv(16480)
        except ConnectionResetError:
            resp_new_connection = self.__make_new_connection()

            if resp_new_connection < 0:
                self.sk.close()

                raise Exception(
                    f'Error in handlesJSON in DelRange: The connection was '
                    f'interrupted at an unexpected time, the reconnection was attempted but failed.'
                )

            resp = self.sk.recv(16880)

        resp = resp.decode(ENCODING_AND_DECODING_TYPE)

        data = json.loads(resp)

        if 'error' in data:
            raise Exception(
                f'Error in handlesJSON DelRange: {str(data["error"])}'
            )

        return data

    def DelAll(self, key: str) -> (str, str, int):
        if not self.__is_connected():
            resp_conn = self.__make_new_connection()

            if resp_conn == -2:
                raise Exception(
                    f'Error in handlesJSON DelAll: There is no replica online'
                )

        msg = json.dumps(
            {
                'function': 'delAll',
                'key': key,
                'value': '',
                'version': -1
            }
        )

        self.sk.send(msg.encode(ENCODING_AND_DECODING_TYPE))

        resp = None

        try:
            resp = self.sk.recv(16480)
        except ConnectionResetError:
            resp_new_connection = self.__make_new_connection()

            if resp_new_connection < 0:
                self.sk.close()

                raise Exception(
                    f'Error in handlesJSON in DelAll: The connection was '
                    f'interrupted at an unexpected time, the reconnection was attempted but failed.'
                )

            resp = self.sk.recv(16880)

        resp = resp.decode(ENCODING_AND_DECODING_TYPE)

        data = json.loads(resp)

        if 'error' in data:
            raise Exception(
                f'Error in handlesJSON DelAll: {str(data["error"])}'
            )

        key_returned = data['key']
        value_returned = data['last_value']
        version_returned = data['last_version']

        return key_returned, value_returned, version_returned

    def Trim(self, key: str) -> (str, str, int):
        if not self.__is_connected():
            resp_conn = self.__make_new_connection()

            if resp_conn == -2:
                raise Exception(
                    f'Error in handlesJSON Trim: There is no replica online'
                )

        msg = json.dumps(
            {
                'function': 'trim',
                'key': key,
                'value': '',
                'version': -1
            }
        )

        self.sk.send(msg.encode(ENCODING_AND_DECODING_TYPE))

        resp = None

        try:
            resp = self.sk.recv(16480)
        except ConnectionResetError:
            resp_new_connection = self.__make_new_connection()

            if resp_new_connection < 0:
                self.sk.close()

                raise Exception(
                    f'Error in handlesJSON in Trim: The connection was '
                    f'interrupted at an unexpected time, the reconnection was attempted but failed.'
                )

            resp = self.sk.recv(16880)

        resp = resp.decode(ENCODING_AND_DECODING_TYPE)

        data = json.loads(resp)

        if 'error' in data:
            raise Exception(
                f'Error in handlesJSON Trim: {str(data["error"])}'
            )

        key_returned = data['key']
        last_value = data['last_value']
        new_version = data['new_version']

        return key_returned, last_value, new_version
