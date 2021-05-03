# rdma client
# const
import pyverbs.cm_enums as ce
import pyverbs.enums as e
# config
import src.config.config as c
# common
from src.common.common import die
from src.common.node import Node
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
            ce.RDMA_CM_EVENT_ESTABLISHED: self._on_connection,
        }

    def request(self):
        self.cid.resolve_addr(self.addr_info, c.TIMEOUT_IN_MS)
        while True:
            self.event = CMEvent(self.event_channel)
            print(self.event.event_type, self.event.event_str())
            event_type = self.event.event_type
            self.event.ack_cm_event()
            if self.event_map[event_type]():
                break

    def close(self):
        self.cid.close()
        self.addr_info.close()

    # resolved addr
    def _on_addr_resolved(self):
        print("address resolved.")
        # resolve_route: will bind context and pd
        self.cid.resolve_route(c.TIMEOUT_IN_MS)
        self.prepare_resource()
        return False

    # on_route_resolved
    def _on_route_resolved(self):
        print("route resolved.")
        conn_param = ConnParam(resources=3, depth=3, retry=3)
        self.cid.connect(conn_param)
        return False

    def _on_connection(self):
        pass
