# rdma client
# const
import pyverbs.cm_enums as ce
import pyverbs.enums as e
# config
import config.config as c
from pyverbs.device import Context
from pyverbs.qp import QPInitAttr, QPCap, QP
from pyverbs.cq import CQ
from pyverbs.cmid import CMID, AddrInfo, CMEventChannel, CMEvent, ConnParam
from pyverbs.wr import RecvWR, SGE
from src.common import GlobalContext, Connection, PollThread, die

addr_res_ctx = None


# TODO: complete the poll
def _poll_cq():
    global addr_res_ctx
    while True:
        # TODO: what is the ctx? None?
        cq = CQ(addr_res_ctx.ctx, 10, None, addr_res_ctx.comp_channel, 0)
        addr_res_ctx.comp_channel.get_cq_event(cq)
        cq.ack_events(1)
        cq.req_notify()
        (npolled, wcs) = cq.poll(1)
        print("npolled:", npolled)
        _on_completion(wcs[0])


def _on_completion(wc):
    conn = wc.wr_id
    print(conn)
    if wc.status != e.IBV_WC_SUCCESS:
        die("on_completion: status is not IBV_WC_SUCCESS")

    if wc.opcode & e.IBV_WC_RECV:
        print("received message:", conn.recv_region)
    elif wc.opcode == e.IBV_WC_SEND:
        print("send completed successfully")
    else:
        die("on_completion: completion isn't a send or a receive")


class RdmaClient:
    def __init__(self, addr, port, name, options=c.OPTIONS):
        # qp init
        # src=addr, ,flags=ce.RAI_PASSIVE
        self.options = options
        self.addr_info = AddrInfo(dst=addr, service=port, port_space=ce.RDMA_PS_TCP)
        # event_channel, cmid and event
        self.event_channel = CMEventChannel()
        self.cid = CMID(creator=self.event_channel)
        self.ctx = Context(name=name)
        self.event = None

        # poll cq
        self.poll_t = PollThread(task=_poll_cq)
        # event loop map config
        self.event_map = {
            ce.RDMA_CM_EVENT_ADDR_RESOLVED: self._on_addr_resolved,
            ce.RDMA_CM_EVENT_ROUTE_RESOLVED: self._on_route_resolved,
        }

    def request(self):
        self.cid.resolve_addr(self.addr_info, c.TIMEOUT_IN_MS)
        # self.cid.resolve_route()
        # self.cid.connect()
        while True:
            self.event = CMEvent(self.event_channel)
            print(self.event.event_type, self.event.event_str())
            # need to copy the event and then ack the event
            # TODO: how to copy the event
            if self.event_map[self.event.event_type]():
                break
            self.event.ack_cm_event()
        self.event.ack_cm_event()

    def close(self):
        self.cid.close()
        self.addr_info.close()

    # resolved addr
    def _on_addr_resolved(self):
        print("address resolved.")
        global addr_res_ctx
        if addr_res_ctx is not None:
            if addr_res_ctx.ctx != self.ctx:
                die("cannot handle events in more than one context.")
            return
        # build_context
        addr_res_ctx = GlobalContext(context=self.ctx)
        # poll_cq
        # self.poll_t.start()
        # build_qp_attr
        qp_options = self.options.get("qp_init")
        cap = QPCap(max_send_wr=qp_options.get("max_send_wr"), max_recv_wr=qp_options.get("max_recv_wr"),
                    max_send_sge=qp_options.get("max_send_sge"), max_recv_sge=qp_options.get("max_recv_sge"))
        self.qp_init_attr = QPInitAttr(qp_type=qp_options.get("qp_type"), cap=cap, scq=addr_res_ctx.cq,
                                       rcq=addr_res_ctx.cq)
        # rdma_create_qp
        self.qp = QP(addr_res_ctx.pd, self.qp_init_attr)

        # register_memory
        conn = Connection(pd=addr_res_ctx.pd, recv_flag=0)
        # post_receives
        sge = SGE(addr=id(conn.recv_region), length=c.BUFFER_SIZE, lkey=conn.recv_mr.lkey)
        wr = RecvWR(num_sge=1, sg=[sge])
        self.qp.post_recv(wr)
        self.cid.resolve_route(c.TIMEOUT_IN_MS)
        return False

    # on_route_resolved
    def _on_route_resolved(self):
        print("route resolved.")
        conn_param = ConnParam()
        self.cid.connect(conn_param)
        return False
