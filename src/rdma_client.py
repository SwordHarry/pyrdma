# rdma client
# const
import sys

import pyverbs.cm_enums as ce
import pyverbs.enums as e
# config
import src.config.config as c
# common
from src.common.common import die
from src.common.node import Node
from src.common.buffer_attr import BufferAttr, serialize, deserialize
# pyverbs
from pyverbs.cmid import CMEvent, ConnParam
from pyverbs.mr import MR


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

    # established, then exchange the meta data
    def _on_established(self) -> bool:
        # need to exchange meta data with server
        # resource_mr: read and write between server and client
        self.resource_mr = MR(self.pd, c.BUFFER_SIZE,
                              e.IBV_ACCESS_LOCAL_WRITE | e.IBV_ACCESS_REMOTE_READ | e.IBV_ACCESS_REMOTE_WRITE)
        # metadata_send_mr: client send the resource_mr attr to server
        self.buffer_attr = BufferAttr(self.resource_mr.buf, c.BUFFER_SIZE, self.resource_mr.lkey)
        buffer_attr_bytes = serialize(self.buffer_attr)
        bytes_len = len(buffer_attr_bytes)
        # print("bytes_len", bytes_len) # 117
        self.metadata_send_mr = MR(self.pd, c.BUFFER_SIZE, e.IBV_ACCESS_LOCAL_WRITE)
        self.metadata_send_mr.write(buffer_attr_bytes, c.BUFFER_SIZE)
        self.cid.post_send(self.metadata_send_mr)
        print("client has post_send metadata")
        self.process_work_completion_events()
        # get the server metadata attr
        self.server_metadata_attr = deserialize(self.metadata_recv_mr.read(c.BUFFER_SIZE, 0))
        print(self.server_metadata_attr)
        return False

    # error: rejected
    def _on_rejected(self) -> bool:
        self.close()
        print("rejected?1")
        return True

    def close(self):
        self.cid.close()
        self.addr_info.close()
