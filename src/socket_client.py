import socket
# const
import pyverbs.enums as e
# common
from pyverbs.cq import CompChannel, CQ
from pyverbs.device import Context
from pyverbs.mr import MR
from pyverbs.pd import PD
from pyverbs.qp import QPCap, QP, QPInitAttr, QPAttr
from pyverbs.wr import SGE, RecvWR, SendWR

from src.common.buffer_attr import BufferAttr, serialize, deserialize
# config
import src.config.config as c
from src.common.common import die


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


class SocketClient:
    def __init__(self, name, addr="127.0.0.1", port=50008, options=c.OPTIONS):
        self.name = name
        self.addr = addr
        self.port = port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.options = options

    def request(self):
        self.socket.connect((self.addr, self.port))
        print("connect successfully")
        # prepare the rdma resource
        self.rdma_ctx = Context(name=self.name)
        self.pd = PD(self.rdma_ctx)
        # comp_channel cq
        self.comp_channel = CompChannel(self.rdma_ctx)
        cqe = self.options["cq_init"]["cqe"]
        self.cq = CQ(self.rdma_ctx, cqe, None, self.comp_channel, 0)
        self.cq.req_notify()

        # build_qp_attr
        qp_options = self.options["qp_init"]
        cap = QPCap(max_send_wr=qp_options["max_send_wr"], max_recv_wr=qp_options["max_recv_wr"],
                    max_send_sge=qp_options["max_send_sge"], max_recv_sge=qp_options["max_recv_sge"])
        qp_init_attr = QPInitAttr(qp_type=qp_options["qp_type"], qp_context=self.rdma_ctx,
                                  cap=cap, scq=self.cq, rcq=self.cq)
        # create_qp and bind in cmid
        # cmid.create_qp(qp_init_attr)
        # memory region
        # metadata_recv_mr: receive the metadata from other node
        # print("len: metadata_attr", len(self.metadata_attr)) # 112
        self.metadata_recv_mr = MR(self.pd, c.BUFFER_SIZE, e.IBV_ACCESS_LOCAL_WRITE)
        # cmid.post_recv(self.metadata_recv_mr)
        # create_qp alone
        qp_attr = QPAttr()
        # QP
        self.qp = QP(self.pd, qp_init_attr)
        sge = SGE(addr=self.metadata_recv_mr.buf, length=c.BUFFER_SIZE, lkey=self.metadata_recv_mr.lkey)
        wr = RecvWR(num_sge=1, sg=[sge])
        # self.qp.to_init(qp_attr)
        self.qp.post_recv(wr)

        # self.qp.to_rtr(qp_attr)
        self.resource_mr = MR(self.pd, c.BUFFER_SIZE,
                              e.IBV_ACCESS_LOCAL_WRITE | e.IBV_ACCESS_REMOTE_READ | e.IBV_ACCESS_REMOTE_WRITE)
        # metadata_send_mr: client send the resource_mr attr to server
        self.buffer_attr = BufferAttr(self.resource_mr.buf, c.BUFFER_SIZE, self.resource_mr.lkey)
        # print("bytes_len", bytes_len) # 117
        # metadata_send_mr: node send the resource_mr attr to others
        self.metadata_send_mr = MR(self.pd, c.BUFFER_SIZE, e.IBV_ACCESS_LOCAL_WRITE)
        buffer_attr_bytes = serialize(self.buffer_attr)
        # bytes_len = len(buffer_attr_bytes)
        # print("bytes_len", bytes_len) # 117
        self.metadata_send_mr.write(buffer_attr_bytes, len(buffer_attr_bytes))
        sge = SGE(addr=self.metadata_send_mr.buf, length=c.BUFFER_SIZE, lkey=self.metadata_send_mr.lkey)
        wr = SendWR(num_sge=1, sg=[sge])
        self.qp.post_send(wr)
        print("client has post_send metadata")
        self.process_work_completion_events()
        # get the server metadata attr
        self.server_metadata_attr = deserialize(self.metadata_recv_mr.read(c.BUFFER_SIZE, 0))
        print(self.server_metadata_attr)

        # exchange done, write message to buffer
        message = "a message from client"
        me_len = len(message)
        self.resource_mr.write(message, me_len)
        sge = SGE(addr=self.resource_mr.buf, length=me_len, rkey=self.server_metadata_attr.remote_stag)
        wr = SendWR(num_sge=1, sg=[sge], opcode=e.IBV_WR_RDMA_WRITE)
        self.qp.post_send(wr)
        self.process_work_completion_events()
        self.socket.close()  # 关闭连接

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
