# const
import pyverbs.enums as e
# config
from pyverbs.cq import CompChannel, CQ
from pyverbs.qp import QPCap, QPInitAttr, QPAttr, QP
from pyverbs.addr import GID, GlobalRoute, AHAttr
from pyverbs.wr import SGE, SendWR, RecvWR

import src.config.config as c
import src.common.msg as m
# common
from src.common.buffer_attr import BufferAttr, FileAttr, serialize
from src.common.common import die
# pyverbs
from pyverbs.device import Context
from pyverbs.mr import MR
from pyverbs.pd import PD


def check_wc_status(wc):
    if wc.status != e.IBV_WC_SUCCESS:
        print(wc)
        die("on_completion: status is not IBV_WC_SUCCESS")
    if wc.opcode & e.IBV_WC_RECV:
        print("received message")
    elif wc.opcode == e.IBV_WC_SEND:
        print("send completed successfully")
    elif wc.opcode == e.IBV_WC_RDMA_WRITE:
        print("write complete")
    elif wc.opcode == e.IBV_WC_RDMA_READ:
        print("read complete")
    else:
        die("completion isn't a send, write, read or a receive")


class SocketNode:
    def __init__(self, name, options=c.OPTIONS):
        self.name = name
        self.options = options
        self.rdma_ctx = None
        self.pd = None
        self.resource_mr = None
        self.read_mr = None
        self.recv_mr = None
        self.file_mr = None
        self.gid = None
        self.buffer_attr = None
        self.comp_channel = None
        self.cq = None
        self.qp = None
        self.file_name=""
        self.fd = None

    def prepare_resource(self):
        self.rdma_ctx = Context(name=self.name)
        self.pd = PD(self.rdma_ctx)
        self.resource_mr = MR(self.pd, c.BUFFER_SIZE,
                              e.IBV_ACCESS_LOCAL_WRITE | e.IBV_ACCESS_REMOTE_READ | e.IBV_ACCESS_REMOTE_WRITE)
        self.read_mr = MR(self.pd, c.BUFFER_SIZE,
                          e.IBV_ACCESS_LOCAL_WRITE | e.IBV_ACCESS_REMOTE_READ | e.IBV_ACCESS_REMOTE_WRITE)
        self.recv_mr = MR(self.pd, c.BUFFER_SIZE,
                          e.IBV_ACCESS_LOCAL_WRITE | e.IBV_ACCESS_REMOTE_READ | e.IBV_ACCESS_REMOTE_WRITE)
        self.file_mr = MR(self.pd, c.FILE_SIZE,
                          e.IBV_ACCESS_LOCAL_WRITE | e.IBV_ACCESS_REMOTE_READ | e.IBV_ACCESS_REMOTE_WRITE)
        # gid
        gid_options = self.options["gid_init"]
        gid = self.rdma_ctx.query_gid(gid_options["port_num"], gid_options["gid_index"])
        # cq
        self.init_cq()
        # qp
        self.init_qp()
        # send the metadata to other
        self.buffer_attr = BufferAttr(self.resource_mr.buf, c.BUFFER_SIZE,
                                      self.resource_mr.lkey, self.resource_mr.rkey,
                                      str(gid), self.qp.qp_num)

    def init_cq(self):
        # comp_channel cq
        self.comp_channel = CompChannel(self.rdma_ctx)
        cqe = self.options["cq_init"]["cqe"]
        self.cq = CQ(self.rdma_ctx, cqe, None, self.comp_channel, 0)
        self.cq.req_notify()

    def init_qp(self):
        qp_options = self.options["qp_init"]
        cap = QPCap(max_send_wr=qp_options["max_send_wr"], max_recv_wr=qp_options["max_recv_wr"],
                    max_send_sge=qp_options["max_send_sge"], max_recv_sge=qp_options["max_recv_sge"])
        qp_init_attr = QPInitAttr(qp_type=qp_options["qp_type"], qp_context=self.rdma_ctx,
                                  cap=cap, scq=self.cq, rcq=self.cq)
        self.qp = QP(self.pd, qp_init_attr)

    def qp2init(self):
        qp_attr = QPAttr(qp_state=e.IBV_QPS_INIT, cur_qp_state=e.IBV_QPS_RESET)
        qp_attr.qp_access_flags = e.IBV_ACCESS_LOCAL_WRITE | e.IBV_ACCESS_REMOTE_READ | e.IBV_ACCESS_REMOTE_WRITE
        self.qp.to_init(qp_attr)
        return self

    def qp2rtr(self, metadata_attr):
        gid_options = self.options["gid_init"]
        qp_attr = QPAttr(qp_state=e.IBV_QPS_RTR, cur_qp_state=e.IBV_QPS_INIT)
        qp_attr.qp_access_flags = e.IBV_ACCESS_LOCAL_WRITE | e.IBV_ACCESS_REMOTE_READ | e.IBV_ACCESS_REMOTE_WRITE
        port_num = gid_options["port_num"]
        remote_gid = GID(metadata_attr.gid)
        gr = GlobalRoute(dgid=remote_gid, sgid_index=gid_options["gid_index"])
        ah_attr = AHAttr(gr=gr, is_global=1, port_num=port_num)
        qp_attr.ah_attr = ah_attr
        qp_attr.dest_qp_num = metadata_attr.qp_num
        self.qp.to_rtr(qp_attr)
        return self

    def qp2rts(self):
        qp_attr = QPAttr(qp_state=e.IBV_QPS_RTS, cur_qp_state=e.IBV_QPS_RTR)
        qp_attr.qp_access_flags = e.IBV_ACCESS_LOCAL_WRITE | e.IBV_ACCESS_REMOTE_READ | e.IBV_ACCESS_REMOTE_WRITE
        self.qp.to_rts(qp_attr)
        return self

    # poll cq
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

    def post_write(self, data, length, rkey, remote_addr, opcode=e.IBV_WR_RDMA_WRITE):
        self.resource_mr.write(data, length)
        sge = SGE(addr=self.resource_mr.buf, length=length, lkey=self.resource_mr.lkey)
        wr = SendWR(opcode=opcode, num_sge=1, sg=[sge, ])
        wr.set_wr_rdma(rkey=rkey, addr=remote_addr)
        self.qp.post_send(wr)

    # TODO: bug: post read can not poll cq?
    def post_read(self, length, rkey, remote_addr):
        sge = SGE(addr=self.read_mr.buf, length=length, lkey=self.read_mr.lkey)
        wr = SendWR(opcode=e.IBV_WR_RDMA_READ, num_sge=1, sg=[sge, ])
        wr.set_wr_rdma(rkey=rkey, addr=remote_addr)
        self.qp.post_send(wr)

    def post_send(self, data, length=0):
        if length == 0:
            length = len(data)
        self.resource_mr.write(data, length)
        sge = SGE(addr=self.resource_mr.buf, length=length, lkey=self.resource_mr.lkey)
        wr = SendWR(opcode=e.IBV_WR_SEND, num_sge=1, sg=[sge, ])
        self.qp.post_send(wr)

    def post_recv(self):
        sge = SGE(addr=self.recv_mr.buf, length=c.BUFFER_SIZE, lkey=self.recv_mr.lkey)
        wr = RecvWR(num_sge=1, sg=[sge, ])
        self.qp.post_recv(wr)

    # passive push file
    def push_file(self, file_path, rkey, remote_addr):
        self.post_recv()
        self.process_work_completion_events()
        msg = self.recv_mr.read(c.BUFFER_SIZE, 0)
        print(msg)
        # write file name
        self.post_write(file_path, len(file_path)+1, rkey, remote_addr, opcode=e.IBV_WR_RDMA_WRITE_WITH_IMM)
        while msg != m.DONE_MSG:
            pass

        # file body
        fd = open(file_path, "rb")
        pass

    def pull_file(self, file_path):
        pass

    # initiative save file
    def save_file(self):
        self.post_recv()
        self.post_send(m.FILE_BEGIN_MSG)
        self.process_work_completion_events()

    # poll cq for file service
    def poll_file(self, poll_count=1):
        self.comp_channel.get_cq_event(self.cq)
        self.cq.req_notify()
        npolled = 0
        while npolled < poll_count:
            (one_poll_count, wcs) = self.cq.poll(num_entries=poll_count)
            npolled += one_poll_count
            if one_poll_count > 0:
                self.cq.ack_events(one_poll_count)
                for wc in wcs:
                    if wc.status == e.IBV_WC_SUCCESS:
                        if wc.opcode == e.IBV_WC_RECV_RDMA_WITH_IMM:
                            # initiative save file
                            size = wc.imm_data
                            if size == 0:
                                self.post_send(m.FILE_DONE_MSG)
                            elif self.file_name:
                                file_stream = self.file_mr.read(size, 0)
                                self.fd.write(file_stream)
                                self.post_recv()
                                self.post_send(m.FILE_READY_MSG)
                            else:
                                file_name = self.file_mr.read(size, 0)
                                self.file_name = file_name
                                self.fd = open(file_name, "wb+")
                                self.post_recv()
                                self.post_send(m.FILE_READY_MSG)
                            pass
                        elif wc.opcode & e.IBV_WC_RECV:
                            if wc.opcode & e.IBV_WC_RECV:
                                pass

    def close(self):
        self.rdma_ctx.close()
        self.pd.close()
        self.resource_mr.close()
        self.read_mr.close()
        self.comp_channel.close()
        self.cq.close()
        self.qp.close()
