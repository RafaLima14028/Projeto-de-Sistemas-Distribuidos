import pickle

import lmdb
import os
import struct
from time import time, sleep


class Database:
    def __init__(self, db_path: str):
        self.db_path = db_path
        os.makedirs(self.db_path, exist_ok=True)

        try:
            self.env = lmdb.open(self.db_path)
        except lmdb.Error as e:
            raise Exception(f'A database creation/opening failure occurred: {str(e)}')

    def put(self, key: str, value: str) -> (str, str, int, int):
        _, old_value_bytes, old_version = self.get(key)

        key_bytes = key.encode('utf-8')
        value_bytes = value.encode('utf-8')

        new_version = int(time())
        new_version_bytes = struct.pack('>Q', new_version)

        with self.env.begin(write=True) as txn:
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
        key_bytes = key.encode('utf-8')
        values = []

        with self.env.begin() as txn:
            try:
                cursor = txn.cursor()
                cursor.get(key_bytes, default=None)
                existing_values = cursor.value()

                if existing_values:
                    existing_values = pickle.loads(existing_values)

                    for version_bytes, value_bytes in existing_values:
                        version = struct.unpack('>Q', version_bytes)[0]
                        values.append((value_bytes.decode('utf-8'), version))
            except lmdb.Error as e:
                raise Exception(f'Failed to fetch data from the database: {str(e)}')

        return values

    def get(self, key: str, version: int = -1) -> (str, str, int):
        values = self.get_all_values_key(key)

        if values == []:
            return '', '', -1

        max_tuple = (None, None, None)

        if version > 0:
            max_version = -1

            for t in values:
                value_tuple = t[0]
                version_tuple = t[1]

                if version_tuple <= version and version_tuple > max_version:
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

    def delete(self, key: str) -> (str, str, int):
        key_bytes = key.encode('utf-8')

        _, last_value, last_version = self.get(key)

        with self.env.begin(write=True) as txn:
            try:
                txn.delete(key_bytes)
            except lmdb.Error as e:
                raise Exception(f'Database deletion failed: {str(e)}')

        return key, last_value, last_version

    def range_scan(self, from_key: str, to_key: str, from_version: int, to_version: int):
        from_key_bytes = from_key.encode('utf-8')
        to_key_bytes = to_key.encode('utf-8')
        from_version_bytes = struct.pack('>Q', from_version)
        to_version_bytes = struct.pack('>Q', to_version)

        result = []

        with self.env.begin() as txn:
            cursor = txn.cursor()

            for key, value in cursor.iternext(keys=True, values=True):
                if from_key_bytes <= key <= to_key_bytes:
                    version = struct.unpack('>Q', value[-8:])[0]

                    if from_version_bytes <= version <= to_version_bytes:
                        value_bytes = value[:-8].decode('utf-8')
                        result.append((key.decode('utf-8'), value_bytes, version))

        return result

    def trim(self, key: str) -> (str, str, int):
        key_bytes = key.encode('utf-8')

        _, last_value, last_version = self.delete(key)

        value_bytes = last_value.encode('utf-8')

        new_version = last_version
        new_version_bytes = struct.pack('>Q', new_version)

        with self.env.begin(write=True) as txn:
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

    def close(self):
        self.env.close()


if __name__ == '__main__':
    db = Database('my_db')

    # print(db.put('key1', 'value1'))
    # sleep(5)
    # print(db.put('key1', 'value2'))
    # sleep(5)
    # print(db.put('key1', 'value3'))
    # sleep(5)
    # print(db.put('key1', 'value4'))
    # sleep(5)
    # print(db.put('key1', 'value5'))
    # sleep(5)
    # print(db.put('key1', 'value6'))

    print(db.get_all_values_key('key2'))
    # print(db.trim('key1'))
    # print(db.get_all_values_key('key1'))
