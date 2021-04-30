# rdma client
# const
import pyverbs.cm_enums as ce
import pyverbs.enums as e
# config
import src.config.config as c
# common
from src.common.common import die
from src.common.node import Node
from src.common.common import PollThread
# pyverbs
from pyverbs.cmid import CMEvent, ConnParam


def _client_on_completion(wc):
    if wc.status != e.IBV_WC_SUCCESS:
        die("on_completion: status is not IBV_WC_SUCCESS")
    if wc.opcode & e.IBV_WC_RECV:
        conn = wc.wr_id
        print(conn)
        print("received message:", conn.recv_region)
    elif wc.opcode == e.IBV_WC_SEND:
        print("send completed successfully")
    else:
        die("on_completion: completion isn't a send or a receive")


class RdmaClient(Node):
    def __init__(self, addr, port, name, options=c.OPTIONS):
        super().__init__(addr, port, name, options=options)

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
        if self.s_ctx is not None:
            if self.s_ctx.ctx != self.ctx:
                die("cannot handle events in more than one context.")
            return
        # poll cq
        # self.build_context(self._poll_cq)
        self.build_context()
        self.cid.resolve_route(c.TIMEOUT_IN_MS)
        return False

    # on_route_resolved
    def _on_route_resolved(self):
        print("route resolved.")
        conn_param = ConnParam()
        self.cid.connect(conn_param)
        return False

    def _poll_cq(self):
        self.poll_t = PollThread(self.s_ctx, on_completion=_client_on_completion, thread_id=2)
        self.poll_t.start()
