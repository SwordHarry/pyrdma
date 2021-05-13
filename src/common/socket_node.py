# const
import pyverbs.enums as e
# config
from pyverbs.cq import CompChannel, CQ
from pyverbs.qp import QPCap, QPInitAttr, QPAttr, QP
from pyverbs.addr import GID, GlobalRoute, AHAttr
from pyverbs.wr import SGE, SendWR

import src.config.config as c
# common
from src.common.buffer_attr import BufferAttr
from src.common.common import die
# pyverbs
from pyverbs.device import Context
from pyverbs.mr import MR
from pyverbs.pd import PD


def _check_wc_status(wc):
    if wc.status != e.IBV_WC_SUCCESS:
        print(wc)
        die("on_completion: status is not IBV_WC_SUCCESS")
    if wc.opcode & e.IBV_WC_RECV:
        print("received message")
    elif wc.opcode == e.IBV_WC_SEND:
        print("send completed successfully")
    else:
        die("on_completion: completion isn't a send or a receive")


class SocketNode:
    def __init__(self, name, options=c.OPTIONS):
        self.name = name
        self.options = options
        self.rdma_ctx = None
        self.pd = None
        self.resource_mr = None
        self.read_mr = None
        self.gid = None
        self.buffer_attr = None
        self.comp_channel = None
        self.cq = None
        self.qp = None

    def prepare_resource(self):
        self.rdma_ctx = Context(name=self.name)
        self.pd = PD(self.rdma_ctx)
        self.resource_mr = MR(self.pd, c.BUFFER_SIZE,
                              e.IBV_ACCESS_LOCAL_WRITE | e.IBV_ACCESS_REMOTE_READ | e.IBV_ACCESS_REMOTE_WRITE)
        self.read_mr = MR(self.pd, c.BUFFER_SIZE,
                          e.IBV_ACCESS_LOCAL_WRITE | e.IBV_ACCESS_REMOTE_READ | e.IBV_ACCESS_REMOTE_WRITE)
        # gid
        gid_options = self.options["gid_init"]
        self.gid = self.rdma_ctx.query_gid(gid_options["port_num"], gid_options["gid_index"])
        # cq
        self.init_cq()
        # qp
        self.init_qp()
        # send the metadata to other
        self.buffer_attr = BufferAttr(self.resource_mr.buf, c.BUFFER_SIZE,
                                      self.resource_mr.lkey, self.resource_mr.rkey,
                                      str(self.gid), self.qp.qp_num)

    def init_cq(self):
        # comp_channel cq
        self.comp_channel = CompChannel(self.rdma_ctx)
        cqe = self.options["cq_init"]["cqe"]
        self.cq = CQ(self.rdma_ctx, cqe, None, self.comp_channel, 0)
        self.cq.req_notify()

    def modify_qp(self, metadata_attr, qp_state=e.IBV_QPS_INIT, cur_qp_state=e.IBV_QPS_RESET):
        gid_options = self.options["gid_init"]
        qp_attr = QPAttr(qp_state=qp_state, cur_qp_state=cur_qp_state)
        port_num = gid_options["port_num"]
        remote_gid = GID(metadata_attr.gid)
        gr = GlobalRoute(dgid=remote_gid, sgid_index=gid_options["gid_index"])
        ah_attr = AHAttr(gr=gr, is_global=1, port_num=port_num)
        qp_attr.ah_attr = ah_attr
        qp_attr.dest_qp_num = metadata_attr.qp_num
        if qp_state == e.IBV_QPS_INIT:
            self.qp.to_init(qp_attr)
        elif qp_state == e.IBV_QPS_RTR:
            self.qp.to_rtr(qp_attr)
        else:
            self.qp.to_rts(qp_attr)

    def init_qp(self):
        qp_options = self.options["qp_init"]
        cap = QPCap(max_send_wr=qp_options["max_send_wr"], max_recv_wr=qp_options["max_recv_wr"],
                    max_send_sge=qp_options["max_send_sge"], max_recv_sge=qp_options["max_recv_sge"])
        qp_init_attr = QPInitAttr(qp_type=qp_options["qp_type"], qp_context=self.rdma_ctx,
                                  cap=cap, scq=self.cq, rcq=self.cq)
        self.qp = QP(self.pd, qp_init_attr)

    def process_work_completion_events(self, poll_count=1):
        # self.comp_channel.get_cq_event(self.cq)
        # self.cq.req_notify()
        npolled = 0
        while npolled < poll_count:
            (one_poll_count, wcs) = self.cq.poll(num_entries=poll_count)
            npolled += one_poll_count
            if one_poll_count > 0:
                for wc in wcs:
                    _check_wc_status(wc)
                self.cq.ack_events(one_poll_count)

    def post_write(self, data, length, rkey, remote_addr):
        self.resource_mr.write(data, length)
        sge = SGE(addr=self.resource_mr.buf, length=length, lkey=self.resource_mr.lkey)
        wr = SendWR(num_sge=1, sg=[sge, ], opcode=e.IBV_WR_RDMA_WRITE)
        wr.set_wr_rdma(rkey=rkey, addr=remote_addr)
        self.qp.post_send(wr)

    def post_read(self, length, rkey, remote_addr):
        sge = SGE(addr=self.resource_mr.buf, length=length, lkey=self.resource_mr.lkey)
        wr = SendWR(num_sge=1, sg=[sge, ], opcode=e.IBV_WR_RDMA_READ)
        wr.set_wr_rdma(rkey=rkey, addr=remote_addr)
        self.qp.post_send(wr)
