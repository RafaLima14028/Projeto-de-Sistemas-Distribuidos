import lmdb
from pysyncobj import SyncObj, SyncObjConf, replicated_sync
import struct
import pickle
from time import time, sleep

from utils import ENCODING_AND_DECODING_TYPE


class Database(SyncObj):
    def __init__(self, selfNode, otherNodes, path):
        self.__conf_pysyncobj = SyncObjConf(
            dynamicMembershipChange=True,
            logCompactionMinTime=86400
        )

        super(Database, self).__init__(
            selfNode,
            otherNodes,
            self.__conf_pysyncobj
        )

        self.__self_node_host = selfNode.split(':')[0]
        self.__self_node_port = int(selfNode.split(':')[1])
        self.__other_nodes = otherNodes
        self.__path_database = f'data_db/{path}/'
        self.__time_factor = 10000

        self.__env = lmdb.open(
            path=self.__path_database,
            sync=True,
            writemap=True,
            metasync=True,
            map_async=True,
            readonly=False,
            max_readers=3,
            max_dbs=3
        )

        self.waitReady()
        sleep(5)

        print(f'The nodes are synchronized: {self.isReady()}')
        print(f'The node {selfNode} has been successfully initialized...')

    def get_host(self):
        return self.__self_node_host

    def get_port(self) -> int:
        return self.__self_node_port

    @replicated_sync(timeout=2)
    def put(self, key: str, value: str) -> (str, str, int, int):
        _, old_value, old_version = self.get(key)

        key_bytes = key.encode(ENCODING_AND_DECODING_TYPE)
        value_bytes = value.encode(ENCODING_AND_DECODING_TYPE)

        new_version = int(time())
        new_version_bytes = struct.pack('d', float(new_version / self.__time_factor))

        try:
            with self.__env.begin(write=True) as txn:
                with txn.cursor() as cursor:
                    if cursor.get(key_bytes, default=None):
                        existing_values = pickle.loads(cursor.value())
                    else:
                        existing_values = []

                    existing_values.append((new_version_bytes, value_bytes))
                    txn.put(key_bytes, pickle.dumps(existing_values))
        except lmdb.Error as e:
            raise Exception(f'A database insertion failure occurred: {str(e)}')

        self.waitReady()

        return key, old_value, old_version, new_version

    def get_all_values_key(self, key: str) -> [(str, int)]:
        key_bytes = key.encode(ENCODING_AND_DECODING_TYPE)
        values = []

        try:
            with self.__env.begin(write=False) as txn:
                cursor = txn.cursor()
                cursor.get(key_bytes, default=None)
                existing_values = cursor.value()

                if existing_values:
                    existing_values = pickle.loads(existing_values)

                    for version_bytes, value_bytes in existing_values:
                        version = struct.unpack('d', version_bytes)[0]
                        values.append(
                            (value_bytes.decode(ENCODING_AND_DECODING_TYPE), int(version * self.__time_factor))
                        )
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
        result = {}

        try:
            with self.__env.begin(write=False) as txn:
                cursor = txn.cursor()

                for key, _ in cursor:
                    key_str = key.decode(ENCODING_AND_DECODING_TYPE)

                    if start_key <= key_str <= end_key and key_str not in result:
                        _, last_value, last_version = self.get(key_str, end_version)

                        if last_version < start_version or last_value == '':
                            result[key_str] = ()
                        else:
                            result[key_str] = (last_version, last_value)
        except lmdb.Error as e:
            raise Exception(f'Failed to fetch data from the database: {str(e)}')

        return result

    @replicated_sync(timeout=2)
    def delete(self, key: str) -> (str, str, int):
        key_bytes = key.encode(ENCODING_AND_DECODING_TYPE)

        _, last_value, last_version = self.get(key)

        try:
            with self.__env.begin(write=True) as txn:
                txn.delete(key_bytes)
        except lmdb.Error as e:
            raise Exception(f'Database delete failed: {str(e)}')

        if last_value == '' and last_version <= 0:
            return '', '', -1
        else:
            return key, last_value, last_version

    @replicated_sync(timeout=2)
    def delRange(self, start_key: str, end_key: str) -> dict:
        values_in_range = dict()

        try:
            with self.__env.begin(write=True) as txn:
                cursor = txn.cursor()

                for key, _ in cursor:
                    key_str = key.decode(ENCODING_AND_DECODING_TYPE)

                    if start_key <= key_str <= end_key and key_str not in values_in_range:
                        _, last_value, last_version = self.get(key_str)

                        values_in_range[key_str] = (last_version, last_value)
        except lmdb.Error as e:
            raise Exception(f'Database delete in range failed: {str(e)}')

        for key in list(values_in_range.keys()):
            try:
                with self.__env.begin(write=True) as txn:
                    txn.delete(key.encode(ENCODING_AND_DECODING_TYPE))
            except lmdb.Error as e:
                raise Exception(f'Database delete failed: {str(e)}')

        return values_in_range

    @replicated_sync(timeout=2)
    def trim(self, key: str) -> (str, str, int):
        key_bytes = key.encode(ENCODING_AND_DECODING_TYPE)

        _, last_value, last_version = self.get(key)

        try:
            with self.__env.begin(write=True) as txn:
                txn.delete(key_bytes)
        except lmdb.Error as e:
            raise Exception(f'Database delete failed: {str(e)}')

        if last_value == '' and last_version <= 0:
            key_bytes = key.encode(ENCODING_AND_DECODING_TYPE)
            value_bytes = last_value.encode(ENCODING_AND_DECODING_TYPE)

            new_version = float(last_version / self.__time_factor)
            new_version_bytes = struct.pack('d', new_version)

            try:
                with self.__env.begin(write=True) as txn:
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
                raise Exception(f'A database trim failure occurred: {str(e)}')

            return key, last_value, last_version
        else:
            key_bytes = key.encode(ENCODING_AND_DECODING_TYPE)
            value_bytes = last_value.encode(ENCODING_AND_DECODING_TYPE)

            new_version = int(time())
            new_version_with_time_factor = float(new_version / self.__time_factor)
            new_version_bytes = struct.pack('d', new_version_with_time_factor)

            try:
                with self.__env.begin(write=True) as txn:
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
                raise Exception(f'A database trim failure occurred: {str(e)}')

            return key, last_value, new_version
