import lmdb
import os
import struct
from time import time


class Database:
    def __init__(self, db_path):
        self.db_path = db_path
        os.makedirs(self.db_path, exist_ok=True)

        try:
            self.env = lmdb.open(self.db_path)
        except lmdb.Error as e:
            raise Exception(f'A database creation/opening failure occurred: {str(e)}')

    def put(self, key: str, value: str) -> None:
        key_bytes = key.encode('utf-8')
        value_bytes = value.encode('utf-8')

        version = int(time())
        version_bytes = struct.pack('>Q', version)

        with self.env.begin(write=True) as txn:
            try:
                txn.put(key_bytes, value_bytes + version_bytes)
            except lmdb.Error as e:
                raise Exception(f'A database insertion failure occurred: {str(e)}')

    def get(self, key: str, version: int = -1) -> (str, str, int):
        if version > 0:
            key_bytes = key.encode('utf-8')
            version_bytes = struct.pack('>Q', version)

            with self.env.begin() as txn:
                try:
                    value_with_version = txn.get(key_bytes + version_bytes)
                except lmdb.Error as e:
                    raise Exception(f'Got failed in the database: {str(e)}')

            if value_with_version is None:
                return None, None, None

            value_bytes = value_with_version[:-8].decode('utf-8')
            version = struct.unpack('>Q', value_with_version[-8:])[0]
        else:
            key_bytes = key.encode('utf-8')

            with self.env.begin() as txn:
                try:
                    value_without_version = txn.get(key_bytes)
                except lmdb.Error as e:
                    raise Exception(f'Got failed in the database: {str(e)}')

            if value_without_version is None:
                return None, None, None

            value_bytes = value_without_version[:-8].decode('utf-8')
            version = struct.unpack('>Q', value_without_version[-8:])[0]

        return key, value_bytes, version

    def delete(self, key: str):
        key_bytes = key.encode('utf-8')

        with self.env.begin(write=True) as txn:
            try:
                old_value = txn.pop(key_bytes, default=None)
            except lmdb.Error as e:
                raise Exception(f'Database deletion failed: {str(e)}')

        if old_value is None:
            return None
        else:
            value_bytes = old_value[:-8].decode('utf-8')
            version = struct.unpack('>Q', old_value[-8:])[0]

            return value_bytes, version

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

    def trim(self, key: str):
        key_bytes = key.encode('utf-8')

        with self.env.begin() as txn:
            cursor = txn.cursor()
            last_version = 0

            for key, value in cursor.iternext(keys=True, values=True):
                if key == key_bytes:
                    version = struct.unpack('>Q', value[-8:])[0]

                    if version > last_version:
                        last_version = version
                        last_value = value[:-8].decode('utf-8')

            # Delete all versions older than the last one
            cursor = txn.cursor()
            for key, value in cursor.iternext(keys=True, values=True):
                if key == key_bytes:
                    version = struct.unpack('>Q', value[-8:])[0]

                    if version < last_version:
                        txn.delete(key)

        return last_value, last_version

    def close(self):
        self.env.close()


if __name__ == '__main__':
    db = Database('my_db')
    db.put('key1', 'value1')

    key, value, version = db.get('key1')

    print(f'Key: {key}, Value: {value}, Version: {version}')
