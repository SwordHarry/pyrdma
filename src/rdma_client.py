# rdma client
# const
import pyverbs.cm_enums as ce
import pyverbs.enums as e
# config
import src.config.config as c
# common
from src.common.common import die
from src.common.node import Node
from src.common.buffer_attr import BufferAttr
# pyverbs
from pyverbs.cmid import CMEvent, ConnParam
from pyverbs.mr import MR


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
            ce.RDMA_CM_EVENT_ESTABLISHED: self._on_established,
            ce.RDMA_CM_EVENT_REJECTED: self._on_rejected,
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

    # resolved addr
    def _on_addr_resolved(self) -> bool:
        print("address resolved.")
        # resolve_route: will bind context and pd
        self.cid.resolve_route(c.TIMEOUT_IN_MS)
        self.prepare_resource(self.cid)
        return False

    # resolve route
    def _on_route_resolved(self) -> bool:
        print("route resolved.")
        conn_param = ConnParam(resources=3, depth=3, retry=3)
        self.cid.connect(conn_param)
        return False

    # established, then exchange the data
    def _on_established(self) -> bool:
        # need to exchange meta data with server
        # resource_mr: read and write between server and client
        self.resource_mr = MR(self.pd, c.BUFFER_SIZE,
                              e.IBV_ACCESS_LOCAL_WRITE | e.IBV_ACCESS_REMOTE_READ | e.IBV_ACCESS_REMOTE_WRITE)
        # metadata_send_mr: client send the resource_mr attr to server
        self.buffer_attr = BufferAttr(self.resource_mr.buf, c.BUFFER_SIZE, self.resource_mr.lkey)
        buffer_attr_bytes = self.buffer_attr.serialize()
        bytes_len = len(buffer_attr_bytes)
        self.metadata_send_mr = MR(self.pd, bytes_len, e.IBV_ACCESS_LOCAL_WRITE)
        # TODO: need to serialize buffer_attr
        self.metadata_send_mr.write(buffer_attr_bytes, bytes_len)
        self.cid.post_send(self.metadata_send_mr)
        print("client has post_send metadata")
        self.process_work_completion_events()
        return False

    # error: rejected
    def _on_rejected(self) -> bool:
        self.close()
        print("rejected?1")
        return True

    def close(self):
        self.cid.close()
        self.addr_info.close()
