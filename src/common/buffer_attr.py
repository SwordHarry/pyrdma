# metadata buffer
import pickle
import sys


class BufferAttr:
    # gid=None, qp_num=0,
    def __init__(self, addr: int = 0, length: int = 0, local_stag="", remote_stag="", gid="", qp_num=0):
        # self.gid = gid
        # self.qp_num = qp_num
        self.addr = addr
        self.length = length
        self.local_stag = local_stag
        self.remote_stag = remote_stag
        self.gid = gid
        self.qp_num = qp_num

    def __str__(self) -> str:
        return "buffer_addr: %s;\nbuffer_len: %s;\nlkey: %s;\nrkey: %s;\ngid: %s;\nqp_num: %s;\n" % \
               (self.addr, self.length, self.local_stag, self.remote_stag, self.gid, self.qp_num)

    def __len__(self) -> int:
        return len(serialize(self))

    def size(self) -> int:
        return sys.getsizeof(self)


def serialize(buffer_attr) -> bytes:
    return pickle.dumps(buffer_attr)


def deserialize(b: bytes):
    return pickle.loads(b)


class FileAttr:
    def __init__(self, file_path, name_len):
        self.file_path = file_path
        self.name_len = name_len

    def __str__(self):
        return "file_path: %s;\nname_len: %s;" % \
               (self.file_path, self.name_len)
