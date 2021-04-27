# rdma client
from pyverbs.qp import QPInitAttr, QPCap
from pyverbs.cmid import CMID, AddrInfo ,CMEventChannel,CMEvent
import pyverbs.cm_enums as ce
from src.common import Context, Connection
import pyverbs.enums as e


class RdmaClient:
    def __init__(self, addr, port, name):
        cap = QPCap(max_recv_wr=1)
        qp_init_attr = QPInitAttr(cap=cap)
        self.addr_info = AddrInfo(src=addr, service=port, dst=addr, port_space=ce.RDMA_PS_TCP)
        self.cid = CMID(creator=self.addr_info, qp_init_attr=qp_init_attr)
        self.ctx = Context(name=name)
        self.conn = Connection(self.ctx.pd)

    def request(self):
        pass

    def close(self):
        self.cid.close()
        self.addr_info.close()
        self.ctx.close()
        self.conn.close()
