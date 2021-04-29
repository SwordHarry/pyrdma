# request struct and config
from pyverbs.pd import PD
from pyverbs.cq import CompChannel, CQ
from pyverbs.mr import MR
import pyverbs.enums as e
import threading


# context of a request
class AddrResolvedContext:
    # ibv_context ctx
    # ibv_pd pd
    # ibv_cq cq
    # ibv_comp_channel comp_channel
    def __init__(self, context=None):
        self.ctx = context
        self.pd = PD(self.ctx)
        self.comp_channel = CompChannel(self.ctx)
        self.cq = CQ(self.ctx, 10, None, self.comp_channel, 0)
        self.cq.req_notify()

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


def die(reason):
    print(reason)
    exit(1)


class PollThread(threading.Thread):
    def __init__(self, task=None, thread_id=1):
        threading.Thread.__init__(self)
        self.threadID = thread_id
        self.task = task

    def run(self):
        if self.task is not None:
            self.task()
