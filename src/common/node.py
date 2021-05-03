# const
import pyverbs.cm_enums as ce
import pyverbs.enums as e
# config
import src.config.config as c
# common
from src.common.connection import Connection
# pyverbs
from pyverbs.device import Context
from pyverbs.cmid import CMID, AddrInfo, CMEventChannel
from pyverbs.qp import QPInitAttr, QPCap, QP, QPAttr
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
        self.pd = None
        self.comp_channel = None
        self.cq = None
        self.event = None
        self.qp = None
        self.conn = None

    def prepare_resource(self):
        # pd, comp_channel, cq, qp

        # protection domains
        self.pd = PD(self.cid.context)
        # completion que
        self.comp_channel = CompChannel(self.cid.context)
        cqe = self.options.get("cq_init").get("cqe")
        self.cq = CQ(self.cid.context, cqe, None, self.comp_channel, 0)
        self.cq.req_notify()
        # build_qp_attr
        qp_options = self.options.get("qp_init")
        cap = QPCap(max_send_wr=qp_options.get("max_send_wr"), max_recv_wr=qp_options.get("max_recv_wr"),
                    max_send_sge=qp_options.get("max_send_sge"), max_recv_sge=qp_options.get("max_recv_sge"))
        qp_init_attr = QPInitAttr(qp_type=qp_options.get("qp_type"), qp_context=self.cid.context, cap=cap, scq=self.cq, rcq=self.cq)
        qp_attr = QPAttr(qp_state=e.IBV_QPT_RC)
        self.qp = QP(self.pd, qp_init_attr, qp_attr)
        # self.cid.create_qp(qp_init_attr)
        # register_memory
        self.conn = Connection(pd=self.pd)
        # post_receives
        sge = SGE(addr=id(self.conn.recv_region), length=c.BUFFER_SIZE, lkey=self.conn.recv_mr.lkey)
        wr = RecvWR(num_sge=1, sg=[sge])
        self.qp.post_recv(wr)
        # self.cid.post_recv(self.conn.recv_mr)
