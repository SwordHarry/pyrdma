# const
import pyverbs.cm_enums as ce
import pyverbs.enums as e
# config
from pyverbs.wr import SGE, RecvWR, SendWR

import src.config.config as c
# common
from src.common.common import die, check_wc_status
from src.common.buffer_attr import BufferAttr, serialize
# pyverbss
from pyverbs.cmid import CMID, AddrInfo, CMEventChannel
from pyverbs.qp import QPInitAttr, QPCap, QP, QPAttr
from pyverbs.mr import MR
from pyverbs.cq import CompChannel, CQ
from pyverbs.pd import PD


# a common node for server or client


class Node:
    def __init__(self, addr, port, is_server=False, options=c.OPTIONS):
        self.options = options
        self.is_server = is_server
        if is_server:
            self.addr_info = AddrInfo(src=addr, src_service=port, port_space=ce.RDMA_PS_TCP, flags=ce.RAI_PASSIVE)
        else:
            self.addr_info = AddrInfo(dst=addr, dst_service=port, port_space=ce.RDMA_PS_TCP)
        # cmid
        self.event_channel = CMEventChannel()
        self.cid = CMID(creator=self.event_channel)
        self.pd = None
        self.comp_channel = None
        self.cq = None
        self.event = None
        self.qp = None
        self.conn = None
        self.recv_mr = None
        # mr
        self.metadata_recv_mr = None
        self.metadata_attr = BufferAttr()
        self.resource_send_mr = None
        self.buffer_attr = None
        self.metadata_send_mr = None

    # if a server, here cmid is an event id; if a client, here cmid is it's cid
    def prepare_resource(self, cmid):
        # protection domains
        self.pd = PD(cmid)
        # comp_channel cq
        self.comp_channel = CompChannel(cmid.context)
        cqe = self.options["cq_init"]["cqe"]
        self.cq = CQ(cmid.context, cqe, None, self.comp_channel, 0)
        self.cq.req_notify()

        # build_qp_attr
        qp_options = self.options["qp_init"]
        cap = QPCap(max_send_wr=qp_options["max_send_wr"], max_recv_wr=qp_options["max_recv_wr"],
                    max_send_sge=qp_options["max_send_sge"], max_recv_sge=qp_options["max_recv_sge"])
        qp_init_attr = QPInitAttr(qp_type=qp_options["qp_type"], qp_context=cmid.context,
                                  cap=cap, scq=self.cq, rcq=self.cq)
        # create_qp and bind in cmid
        cmid.create_qp(qp_init_attr)
        # memory region
        # metadata_recv_mr: receive the metadata from other node
        self.metadata_recv_mr = MR(self.pd, c.BUFFER_SIZE, e.IBV_ACCESS_LOCAL_WRITE)
        cmid.post_recv(self.metadata_recv_mr)
        # create_qp alone
        # qp_attr = QPAttr(qp_state=e.IBV_QPT_RC)
        # QP
        # self.qp = QP(self.pd, qp_init_attr)
        # sge = SGE(addr=self.metadata_recv_mr.buf, length=c.BUFFER_SIZE, lkey=self.metadata_recv_mr.lkey)
        # wr = RecvWR(num_sge=1, sg=[sge])
        # self.qp.post_recv(wr)

    def init_mr(self, buffer_size: int):
        # init metadata and resource mr
        # resource_send_mr: read and write between server and client
        self.resource_send_mr = MR(self.pd, buffer_size,
                                   e.IBV_ACCESS_LOCAL_WRITE | e.IBV_ACCESS_REMOTE_READ | e.IBV_ACCESS_REMOTE_WRITE)
        # metadata_send_mr: client send the resource_send_mr attr to server
        self.buffer_attr = BufferAttr(addr=self.resource_send_mr.buf, length=buffer_size,
                                      local_stag=self.resource_send_mr.lkey, remote_stag=self.resource_send_mr.rkey)
        # metadata_send_mr: node send the resource_send_mr attr to others
        self.metadata_send_mr = MR(self.pd, buffer_size, e.IBV_ACCESS_LOCAL_WRITE)

    def process_work_completion_events(self, poll_count=1):
        self.comp_channel.get_cq_event(self.cq)
        self.cq.req_notify()
        npolled = 0
        while npolled < poll_count:
            (one_poll_count, wcs) = self.cq.poll(num_entries=poll_count)
            npolled += one_poll_count
            if one_poll_count > 0:
                for wc in wcs:
                    check_wc_status(wc)
                self.cq.ack_events(one_poll_count)
