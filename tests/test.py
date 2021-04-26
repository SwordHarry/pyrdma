from pyverbs.qp import QPInitAttr, QPCap
from pyverbs.cmid import CMID, AddrInfo
import pyverbs.cm_enums as ce

cap = QPCap(max_recv_wr=1)
qp_init_attr = QPInitAttr(cap=cap)
addr = "192.168.236.128"
port = "7471"
# src_service=port,
sai = AddrInfo(src=addr, service=port, port_space=ce.RDMA_PS_TCP, flags=ce.RAI_PASSIVE)
# sai = AddrInfo(addr, port, ce.RDMA_PS_TCP, ce.RAI_PASSIVE)
sid = CMID(creator=sai, qp_init_attr=qp_init_attr)
sid.listen()
new_id = sid.get_request()
c = new_id.accept()
print(c)
