from src.config import config as c
from pyverbs.mr import MR
import pyverbs.enums as e


# connection of a request
class Connection:
    # ibv_qp qp
    # ibv_mr recv_mr
    # ibv_mr send_mr
    def __init__(self, pd=None, recv_flag=e.IBV_ACCESS_LOCAL_WRITE, send_flag=e.IBV_ACCESS_LOCAL_WRITE,
                 mr_len=c.BUFFER_SIZE):
        self.recv_mr = MR(pd, mr_len, recv_flag)
        self.send_mr = MR(pd, mr_len, send_flag)
        self.recv_region = ""
        self.send_region = ""

    def close(self):
        self.recv_mr.close()
        self.send_mr.close()
