# request struct and config
import pyverbs.device as d
from pyverbs.pd import PD
from pyverbs.cq import CompChannel, CQ
from pyverbs.mr import MR
import pyverbs.enums as e


# context of a request
class Context:
    # ibv_context ctx
    # ibv_pd pd
    # ibv_cq cq
    # ibv_comp_channel comp_channel
    def __init__(self, name=""):
        self.ctx = d.Context(name=name)
        self.pd = PD(self.ctx)
        self.comp_channel = CompChannel(self.ctx)
        self.cq = CQ(self.ctx, 2, None, self.comp_channel, 0)
        print(self.ctx, self.pd, self.comp_channel, self.cq)


# connection of a request
class Connection:
    # ibv_qp qp
    # ibv_mr recv_mr
    # ibv_mr send_mr
    def __init__(self, pd=None, mr_len=1000):
        self.recv_mr = MR(pd, mr_len, e.IBV_ACCESS_LOCAL_WRITE)
        self.send_mr = MR(pd, mr_len, 0)
        print(self.recv_mr, self.send_mr)


