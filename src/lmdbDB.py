import lmdb
from pysyncobj import SyncObj
import struct
import pickle
from time import time

from utils import ENCODING_AND_DECODING_TYPE


class Database(SyncObj):
    def __init__(self, port, primary, secundary):
        super(Database, self).__init__(primary, secundary)
        self.path_database = f'my_db/{port}/'

    def __open_db(self):
        db = None

        try:
            db = lmdb.open(self.path_database, sync=True, writemap=True)
        except lmdb.Error as e:
            raise Exception(f'Database open failed: {str(e)}')

        return db

    def put(self, key: str, value: str) -> (str, str, int, int):
        _, old_value_bytes, old_version = self.get(key)

        db = self.__open_db()

        key_bytes = key.encode(ENCODING_AND_DECODING_TYPE)
        value_bytes = value.encode(ENCODING_AND_DECODING_TYPE)

        new_version = int(time())
        new_version_bytes = struct.pack('>Q', new_version)

        with db.begin(write=True) as txn:
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
                raise Exception(f'A database insertion failure occurred: {str(e)}')

        return key, old_value_bytes, old_version, new_version

    def get_all_values_key(self, key: str) -> [(str, int)]:
        db = self.__open_db()

        key_bytes = key.encode(ENCODING_AND_DECODING_TYPE)
        values = []

        with db.begin() as txn:
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

        max_tuple = (None, None, None)

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

        db = self.__open_db()

        if start_version <= 0 or end_version <= 0:
            with db.begin() as tnx:
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
                with db.begin() as tnx:
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

    def delete(self, key: str) -> (str, str, int):
        key_bytes = key.encode(ENCODING_AND_DECODING_TYPE)

        _, last_value, last_version = self.get(key)

        db = self.__open_db()

        with db.begin(write=True) as txn:
            try:
                txn.delete(key_bytes)
            except lmdb.Error as e:
                raise Exception(f'Database delete failed: {str(e)}')

        return key, last_value, last_version

    def delRange(self, start_key: str, end_key: str) -> dict:
        values_in_range = dict()

        db = self.__open_db()

        with db.begin(write=True) as txn:
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
                raise Exception(f'Database delete in range failed: {str(e)}')

        return values_in_range

    def trim(self, key: str) -> (str, str, int):
        key_bytes = key.encode(ENCODING_AND_DECODING_TYPE)

        _, last_value, last_version = self.delete(key)

        value_bytes = last_value.encode(ENCODING_AND_DECODING_TYPE)

        new_version = last_version
        new_version_bytes = struct.pack('>Q', new_version)

        db = self.__open_db()

        with db.begin(write=True) as txn:
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
                raise Exception(f'A database insertion failure occurred: {str(e)}')

        return key, last_value, new_version


if __name__ == '__main__':
    server = Database(
        '50055',
        'localhost:50056',
        ['localhost:50057', 'localhost:50058']
    )

    print(server.put('Rafael', 'Alves'))
    print(server.get('Rafael'))
