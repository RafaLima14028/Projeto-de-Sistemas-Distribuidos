import socket
import pickle
from threading import Thread
from pysyncobj import SyncObj, replicated


class DistributedKeyValueStore(SyncObj):
    def __init__(self, selfNodeAddr: (str, int), otherNodes: [(str, int)]):
        super(DistributedKeyValueStore, self).__init__(selfNodeAddr, otherNodes)
        self.data = {}

    @replicated
    def set(self, key: str, value: str) -> None:
        self.data[key] = value

    @replicated
    def get(self, key: str) -> str | None:
        return self.data.get(key)
