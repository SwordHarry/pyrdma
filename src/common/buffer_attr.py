# metadata buffer
import pickle
import sys


class BufferAttr:
    def __init__(self, addr: int, length: int, local_stag="", remote_stag=""):
        self.addr = addr
        self.length = length
        self.local_stag = local_stag
        self.remote_stag = remote_stag

    def __str__(self) -> str:
        return "%s %s %s %s" % (self.addr, self.length, self.local_stag, self.remote_stag)

    def size(self) -> int:
        return sys.getsizeof(self)

    def serialize(self) -> bytes:
        return pickle.dumps(self)
