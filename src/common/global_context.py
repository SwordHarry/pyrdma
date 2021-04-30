from pyverbs.pd import PD
from pyverbs.cq import CompChannel, CQ


# context of a request
class GlobalContext:
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
