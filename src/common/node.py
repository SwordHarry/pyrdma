# const
import pyverbs.cm_enums as ce
import pyverbs.enums as e
# config
import src.config.config as c
# common
from src.common.common import die
from src.common.buffer_attr import BufferAttr
# pyverbss
from pyverbs.cmid import CMID, AddrInfo, CMEventChannel
from pyverbs.qp import QPInitAttr, QPCap, QP
from pyverbs.mr import MR
from pyverbs.cq import CompChannel, CQ
from pyverbs.pd import PD


# a common node for server or client

def _check_wc_status(wc):
    if wc.status != e.IBV_WC_SUCCESS:
        die("on_completion: status is not IBV_WC_SUCCESS")
    if wc.opcode & e.IBV_WC_RECV:
        print("received message")
    elif wc.opcode == e.IBV_WC_SEND:
        print("send completed successfully")
    else:
        die("on_completion: completion isn't a send or a receive")


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
        self.recv_mr = None
        # mr
        self.metadata_recv_mr = None
        self.metadata_attr = BufferAttr()

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
        # print("len: metadata_attr", len(self.metadata_attr)) # 112
        self.metadata_recv_mr = MR(self.pd, c.BUFFER_SIZE, e.IBV_ACCESS_LOCAL_WRITE)
        cmid.post_recv(self.metadata_recv_mr)
        # create_qp alone
        # qp_attr = QPAttr(qp_state=e.IBV_QPT_RC)
        # QP
        # self.qp = QP(self.pd, qp_init_attr)
        # sge = SGE(addr=id(selsf.metadata_recv_mr), length=c.BUFFER_SIZE, lkey=self.metadata_recv_mr.lkey)
        # wr = RecvWR(num_sge=1, sg=[sge])
        # self.qp.post_recv(wr)

    def process_work_completion_events(self):
        print("getting cq event")
        self.comp_channel.get_cq_event(self.cq)
        self.cq.req_notify()
        (npolled, wcs) = self.cq.poll()
        print("poll has completed, npolled: ", npolled, "wcs: ", wcs)
        if npolled > 0:
            for wc in wcs:
                _check_wc_status(wc)
            self.cq.ack_events(npolled)

