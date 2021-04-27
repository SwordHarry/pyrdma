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

    def close(self):
        self.cq.close()
        self.comp_channel.close()
        self.pd.close()
        self.ctx.close()


# connection of a request
class Connection:
    # ibv_qp qp
    # ibv_mr recv_mr
    # ibv_mr send_mr
    def __init__(self, pd=None, recv_flag=e.IBV_ACCESS_REMOTE_READ, send_flag=e.IBV_ACCESS_LOCAL_WRITE, mr_len=1000):
        self.recv_mr = MR(pd, mr_len, recv_flag)
        self.send_mr = MR(pd, mr_len, send_flag)

    def close(self):
        self.recv_mr.close()
        self.send_mr.close()
