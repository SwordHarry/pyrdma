from pyverbs.qp import QPInitAttr, QPCap
from pyverbs.cmid import CMID, AddrInfo
import pyverbs.cm_enums as ce

cap = QPCap(max_recv_wr=1)
qp_init_attr = QPInitAttr(cap=cap)
addr = '192.168.236.128'
port = '7471'

# Active side
cai = AddrInfo(src=addr, service=port, dst=addr, port_space=ce.RDMA_PS_TCP)
# cai = AddrInfo(addr, port, ce.RDMA_PS_TCP)
cid = CMID(creator=cai, qp_init_attr=qp_init_attr)
c = cid.connect()  # send connection request to passive addr
print(c)
