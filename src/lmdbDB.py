import lmdb
from pysyncobj import SyncObj, replicated, replicated_sync
import struct
import pickle
from time import time, sleep
import threading

from utils import ENCODING_AND_DECODING_TYPE


class Database(SyncObj):
    def __init__(self, primary: str, secundary: [str]):
        super(Database, self).__init__(primary, secundary)
        self.__path_database = f'data_db/'

        self.__env = lmdb.open(
            path=self.__path_database,
            sync=True,
            writemap=True,
            map_async=True,
            readonly=False,
            max_readers=1
        )

        self.__lock = threading.Lock()

        sleep(5)

        print(self.__env.info())
        print(self.__env.stat())
        print('Initialized the database')

    @replicated_sync
    def put(self, key: str, value: str) -> (str, str, int, int):
        _, old_value_bytes, old_version = self.get(key)

        key_bytes = key.encode(ENCODING_AND_DECODING_TYPE)
        value_bytes = value.encode(ENCODING_AND_DECODING_TYPE)

        new_version = int(time())
        new_version_bytes = struct.pack('>Q', new_version)

        with self.__lock:
            with self.__env.begin(write=True) as txn:
                try:
                    cursor = txn.cursor()
                    cursor.get(key_bytes, default=None)
                    existing_values = cursor.value()

                    if existing_values:
                        existing_values = pickle.loads(existing_values)
                    else:
                        existing_values = []

                    existing_values.append((new_version_bytes, value_bytes))
                    txn.put(key_bytes, pickle.dumps(existing_values))
                except lmdb.Error as e:
                    txn.abort()
                    raise Exception(f'A database insertion failure occurred: {str(e)}')

        sleep(5)

        return key, old_value_bytes, old_version, new_version

    def get_all_values_key(self, key: str) -> [(str, int)]:
        key_bytes = key.encode(ENCODING_AND_DECODING_TYPE)
        values = []

        with self.__env.begin() as txn:
            try:
                cursor = txn.cursor()
                cursor.get(key_bytes, default=None)
                existing_values = cursor.value()

                if existing_values:
                    existing_values = pickle.loads(existing_values)

                    for version_bytes, value_bytes in existing_values:
                        version = struct.unpack('>Q', version_bytes)[0]
                        values.append((value_bytes.decode(ENCODING_AND_DECODING_TYPE), version))
            except lmdb.Error as e:
                raise Exception(f'Failed to fetch data from the database: {str(e)}')

        return values

    def get(self, key: str, version: int = -1) -> (str, str, int):
        values = self.get_all_values_key(key)

        if not values:
            return '', '', -1

        max_tuple = ('', '', -1)

        if version > 0:
            max_version = -1

            for t in values:
                value_tuple = t[0]
                version_tuple = t[1]

                if version >= version_tuple > max_version:
                    max_version = version_tuple
                    max_tuple = (key, value_tuple, version_tuple)
        else:
            max_version = -1

            for t in values:
                value_tuple = t[0]
                version_tuple = t[1]

                if version_tuple > max_version:
                    max_version = version_tuple
                    max_tuple = (key, value_tuple, version_tuple)

        return max_tuple

    def getRange(self, start_key: str, end_key: str, start_version: int = -1, end_version: int = -1) -> dict:
        values_in_range = dict()

        if start_version <= 0 or end_version <= 0:
            with self.__env.begin() as tnx:
                try:
                    for key, values in tnx.cursor():
                        key = key.decode(ENCODING_AND_DECODING_TYPE)
                        _, value_returned, version_returned = self.get(key)

                        if key in values_in_range:
                            values_in_range[key].append((version_returned, value_returned))
                        else:
                            values_in_range[key] = [(version_returned, value_returned)]
                except lmdb.Error as e:
                    raise Exception(f'Database get in range failed: {str(e)}')
        else:
            max_request_version = max(start_version, end_version)

            if start_version <= 0 or end_version <= 0:
                with self.__env.begin() as tnx:
                    try:
                        for key, values in tnx.cursor():
                            key = key.decode(ENCODING_AND_DECODING_TYPE)

                            if start_key <= key <= end_key:
                                for version, value in values:
                                    if version <= max_request_version:
                                        if key in values_in_range:
                                            values_in_range[key].append((version, value))
                                        else:
                                            values_in_range[key] = [(version, value)]
                    except lmdb.Error as e:
                        raise Exception(f'Database get in range failed: {str(e)}')

        return values_in_range

    @replicated
    def delete(self, key: str) -> (str, str, int):
        key_bytes = key.encode(ENCODING_AND_DECODING_TYPE)

        _, last_value, last_version = self.get(key)

        with self.__env.begin(write=True) as txn:
            try:
                txn.delete(key_bytes)
            except lmdb.Error as e:
                txn.abort()
                raise Exception(f'Database delete failed: {str(e)}')

        if last_value == '' and last_version <= 0:
            return '', '', -1
        else:
            return key, last_value, last_version

    @replicated
    def delRange(self, start_key: str, end_key: str) -> dict:
        values_in_range = dict()

        with self.__env.begin(write=True) as txn:
            try:
                cursor = txn.cursor()

                for key, value in cursor:
                    key_str = key.decode(ENCODING_AND_DECODING_TYPE)

                    if start_key <= key_str <= end_key:
                        _, last_value, last_version = self.get(key_str)

                        if key_str in values_in_range:
                            values_in_range[key_str].append((last_version, last_value))
                        else:
                            values_in_range[key_str] = [(last_version, last_value)]

                        cursor.delete()
            except lmdb.Error as e:
                txn.abort()
                raise Exception(f'Database delete in range failed: {str(e)}')

        return values_in_range

    @replicated
    def trim(self, key: str) -> (str, str, int):
        key_bytes = key.encode(ENCODING_AND_DECODING_TYPE)

        _, last_value, last_version = self.get(key)

        with self.__env.begin(write=True) as txn:
            try:
                txn.delete(key_bytes)
            except lmdb.Error as e:
                txn.abort()
                raise Exception(f'Database delete failed: {str(e)}')

        if last_value == '' and last_version <= 0:
            key_bytes = key.encode(ENCODING_AND_DECODING_TYPE)
            value_bytes = last_value.encode(ENCODING_AND_DECODING_TYPE)

            new_version = last_version
            new_version_bytes = struct.pack('>Q', new_version)

            with self.__env.begin(write=True) as txn:
                try:
                    cursor = txn.cursor()
                    cursor.get(key_bytes, default=None)
                    existing_values = cursor.value()

                    if existing_values:
                        existing_values = pickle.loads(existing_values)
                    else:
                        existing_values = []

                    existing_values.append((new_version_bytes, value_bytes))
                    txn.put(key_bytes, pickle.dumps(existing_values))
                except lmdb.Error as e:
                    txn.abort()
                    raise Exception(f'A database trim failure occurred: {str(e)}')

            return key, last_value, last_version
        else:
            key_bytes = key.encode(ENCODING_AND_DECODING_TYPE)
            value_bytes = last_value.encode(ENCODING_AND_DECODING_TYPE)

            new_version = last_version
            new_version_bytes = struct.pack('>Q', new_version)

            with self.__env.begin(write=True) as txn:
                try:
                    cursor = txn.cursor()
                    cursor.get(key_bytes, default=None)
                    existing_values = cursor.value()

                    if existing_values:
                        existing_values = pickle.loads(existing_values)
                    else:
                        existing_values = []

                    existing_values.append((new_version_bytes, value_bytes))
                    txn.put(key_bytes, pickle.dumps(existing_values))
                except lmdb.Error as e:
                    txn.abort()
                    raise Exception(f'A database trim failure occurred: {str(e)}')

            return key, last_value, new_version


if __name__ == '__main__':
    server = Database(
        'localhost:50053',
        ['localhost:50053', 'localhost:50053']
    )

    print()

    # print(f'Put: {server.put("Rafael", "teste2")}')
    # print(f'All values: {server.get_all_values_key("Rafael")}')
    # print(f'Get: {server.get("Rafael")}')

    # print(f'Trim: {server.trim("Rafael")}')
    # print(f'All values: {server.get_all_values_key("Rafael")}')

    # print(f'All values: {server.get_all_values_key("Rafael")}')
    # print(f'Del: {server.delete("Rafael")}')
    # print(f'All values: {server.get_all_values_key("Rafael")}')

    print(f'All values: {server.get_all_values_key("Rafael")}')
    print(f'Trim: {server.trim("Rafael")}')
    print(f'All values: {server.get_all_values_key("Rafael")}')
