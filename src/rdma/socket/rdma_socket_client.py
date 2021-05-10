from pyverbs.cmid import CMID, AddrInfo
from pyverbs.qp import QPInitAttr, QPCap
import src.config.config as c
import pyverbs.cm_enums as ce
from src.common.rdma_node import Node


class RdmaSocketClient:
    def __init__(self, addr, port, options=c.OPTIONS):
        addr_info = AddrInfo(dst=addr, dst_service=port, port_space=ce.RDMA_PS_TCP)
        qp_options = options["qp_init"]
        cap = QPCap(max_send_wr=qp_options["max_send_wr"], max_recv_wr=qp_options["max_recv_wr"],
                    max_send_sge=qp_options["max_send_sge"], max_recv_sge=qp_options["max_recv_sge"])
        qp_init_attr = QPInitAttr(qp_type=qp_options["qp_type"], cap=cap)
        self.sid = CMID(creator=addr_info, qp_init_attr=qp_init_attr)

    def request(self):
        self.sid.connect()
        print("connect success")
        self.sid.close()
