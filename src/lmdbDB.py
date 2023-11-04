import lmdb
from pysyncobj import SyncObj, SyncObjConf, replicated_sync, node
import struct
import pickle
from time import time, sleep

from src.utils import ENCODING_AND_DECODING_TYPE

TIME_FACTOR = 10000


class Database(SyncObj):
    def __init__(self, selfNode, path):
        super(Database, self).__init__(
            selfNode,
            [],
            SyncObjConf(
                dynamicMembershipChange=True
            )
        )

        self.__self_node = selfNode
        self.__nodes_of_some_cluster = []
        self.__path_database = f'data_db/{path}/'

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

        sleep(5)

        print(f'The nodes are synchronized: {self.isReady()}')
        print(f'The node {selfNode} has been successfully initialized...')
        print()

    def get_port(self) -> int:
        return int(self.__self_node.split(':')[1])

    def del_node(self, host_port: str) -> int:
        """
        Returns:
             1 - Success
             2 - Already unconnected
            -1 - Error
        """

        if host_port in self.__nodes_of_some_cluster and host_port != self.__self_node:
            try:
                self.removeNodeFromCluster(host_port)
                self.__nodes_of_some_cluster.remove(host_port)
                sleep(2)

                print(f'Unconnected with {host_port}')

                return 1
            except Exception:
                return -1
        else:
            return 2

    def add_new_node(self, host_port: str) -> int:
        """
        Returns:
             1 - Success
             2 - Already connected
            -1 - Error
        """

        if host_port not in self.__nodes_of_some_cluster and host_port != self.__self_node:
            try:
                self.addNodeToCluster(host_port)
                self.__nodes_of_some_cluster.append(host_port)
                sleep(2)

                print(f'Connected with {host_port}')

                return 1
            except Exception:
                return -1
        else:
            return 2

    @replicated_sync(timeout=2)
    def put(self, key: str, value: str) -> (str, str, int, int):
        _, old_value, old_version = self.get(key)

        print(key, old_value, old_version)

        key_bytes = key.encode(ENCODING_AND_DECODING_TYPE)
        value_bytes = value.encode(ENCODING_AND_DECODING_TYPE)

        new_version = int(time())
        new_version_bytes = struct.pack('d', float(new_version / TIME_FACTOR))

        try:
            with self.__env.begin(write=True) as txn:
                with txn.cursor() as cursor:
                    if cursor.get(key_bytes, default=None):
                        existing_values = pickle.loads(cursor.value())
                    else:
                        existing_values = []
                    # cursor.get(key_bytes, default=None)
                    # existing_values = cursor.value()
                    #
                    # if existing_values:
                    #     existing_values = pickle.loads(existing_values)
                    # else:
                    #     existing_values = []

                    existing_values.append((new_version_bytes, value_bytes))
                    txn.put(key_bytes, pickle.dumps(existing_values))
        except lmdb.Error as e:
            raise Exception(f'A database insertion failure occurred: {str(e)}')

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
                        values.append((value_bytes.decode(ENCODING_AND_DECODING_TYPE), int(version * 10000)))
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
            try:
                with self.__env.begin(write=False) as tnx:
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
                try:
                    with self.__env.begin(write=False) as tnx:
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

    @replicated_sync
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

    @replicated_sync
    def delRange(self, start_key: str, end_key: str) -> dict:
        values_in_range = dict()

        try:
            with self.__env.begin(write=True) as txn:
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
            raise Exception(f'Database delete in range failed: {str(e)}')

        return values_in_range

    @replicated_sync
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

            new_version = float(last_version / TIME_FACTOR)
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
            new_version_with_time_factor = float(new_version / TIME_FACTOR)
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
