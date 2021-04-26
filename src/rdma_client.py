# rdma client
from pyverbs.qp import QPInitAttr, QPCap
from pyverbs.cmid import CMID, AddrInfo
import pyverbs.cm_enums as ce


class RdmaClient:
    def __init__(self, addr, port):
        cap = QPCap(max_recv_wr=1)
        qp_init_attr = QPInitAttr(cap=cap)
        sai = AddrInfo(src=addr, service=port, dst=addr,port_space=ce.RDMA_PS_TCP)
        self.cid = CMID(creator=sai, qp_init_attr=qp_init_attr)

    def request(self):
        self.cid.connect()  # send a connect request
