# const
import pyverbs.cm_enums as ce
# config
import src.config.config as c
# common
from src.common.global_context import GlobalContext
from src.common.connection import Connection
# pyverbs
from pyverbs.device import Context
from pyverbs.cmid import CMID, AddrInfo, CMEventChannel
from pyverbs.qp import QPInitAttr, QPCap, QP
from pyverbs.wr import RecvWR, SGE
from pyverbs.cq import CompChannel, CQ
from pyverbs.pd import PD


# a common node for server or client

class Node:
    def __init__(self, addr, port, name, is_server=False, options=c.OPTIONS):
        self.options = options
        self.is_server = is_server
        if is_server:
            self.addr_info = AddrInfo(src=addr, service=port, port_space=ce.RDMA_PS_TCP, flags=ce.RAI_PASSIVE)
        else:
            self.addr_info = AddrInfo(dst=addr, service=port, port_space=ce.RDMA_PS_TCP)
        # cmid
        self.event_channel = CMEventChannel()
        self.cid = CMID(creator=self.event_channel)
        # rdma context
        self.ctx = Context(name=name)
        # completion que
        self.comp_channel = CompChannel(self.ctx)
        self.cq = CQ(self.ctx, 10, None, self.comp_channel, 0)
        # protection domains
        self.pd = PD(self.ctx)
        self.event = None
        self.qp = None

    def build_context(self, poll_cq=None):
        # build_context
        # poll_cq
        # TODO: poll_Cq
        if poll_cq is not None:
            poll_cq()
        # build_qp_attr
        qp_options = self.options.get("qp_init")
        cap = QPCap(max_send_wr=qp_options.get("max_send_wr"), max_recv_wr=qp_options.get("max_recv_wr"),
                    max_send_sge=qp_options.get("max_send_sge"), max_recv_sge=qp_options.get("max_recv_sge"))
        qp_init_attr = QPInitAttr(qp_type=qp_options.get("qp_type"), cap=cap, scq=self.cq,
                                  rcq=self.cq)
        # rdma_create_qp
        self.qp = QP(self.pd, qp_init_attr)
        # register_memory
        conn = Connection(pd=self.pd, send_flag=0)
        # post_receives
        sge = SGE(addr=id(conn.recv_region), length=c.BUFFER_SIZE, lkey=conn.recv_mr.lkey)
        wr = RecvWR(num_sge=1, sg=[sge])
        self.qp.post_recv(wr)
