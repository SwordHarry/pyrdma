# rdma client
# const
import sys

import pyverbs.cm_enums as ce
import pyverbs.enums as e
# config
from pyverbs.wr import SGE, SendWR

import src.config.config as c
# common
from src.common.common import print_info
from src.common.rdma_node import Node
from src.common.buffer_attr import BufferAttr, serialize, deserialize
# pyverbs
from pyverbs.cmid import CMEvent, ConnParam
from pyverbs.mr import MR


class RdmaClient(Node):
    def __init__(self, addr, port, options=c.OPTIONS):
        super().__init__(addr, port, options=options)

        # event loop map config
        self.event_map = {
            ce.RDMA_CM_EVENT_ADDR_RESOLVED: self._on_addr_resolved,
            ce.RDMA_CM_EVENT_ROUTE_RESOLVED: self._on_route_resolved,
            ce.RDMA_CM_EVENT_ESTABLISHED: self._on_established,
            ce.RDMA_CM_EVENT_DISCONNECTED: self._on_disconnected,
            ce.RDMA_CM_EVENT_REJECTED: self._on_rejected,
        }

    def request(self):
        self.cid.resolve_addr(self.addr_info, c.TIMEOUT_IN_MS)
        while True:
            event = CMEvent(self.event_channel)
            print(event.event_type, event.event_str())
            event_type = event.event_type
            if self.event_map[event_type]():
                break
            event.ack_cm_event()

    # resolved addr
    def _on_addr_resolved(self) -> bool:
        # resolve_route: will bind context and pd
        self.cid.resolve_route(c.TIMEOUT_IN_MS)
        self.prepare_resource(self.cid)
        return False

    # resolve route
    def _on_route_resolved(self) -> bool:
        conn_param = ConnParam(resources=3, depth=3, retry=3)
        self.cid.connect(conn_param)
        return False

    # established, then exchange the meta data
    def _on_established(self) -> bool:
        # need to exchange meta data with server
        # init resource and metadata send mr, buffer_attr
        self.init_mr(c.BUFFER_SIZE)
        buffer_attr_bytes = serialize(self.buffer_attr)
        # bytes_len = len(buffer_attr_bytes)
        # print("bytes_len", bytes_len) # 117
        self.metadata_send_mr.write(buffer_attr_bytes, len(buffer_attr_bytes))
        # sge = SGE(addr=self.metadata_send_mr.buf, length=c.BUFFER_SIZE, lkey=self.metadata_send_mr.lkey)
        # wr = SendWR(num_sge=1, sg=[sge])
        # self.qp.post_send(wr)
        self.cid.post_send(self.metadata_send_mr)
        self.process_work_completion_events(poll_count=2)
        server_metadata_attr_bytes = self.metadata_recv_mr.read(c.BUFFER_SIZE, 0)
        self.server_metadata_attr = deserialize(server_metadata_attr_bytes)
        print_info("server metadata attr:\n"+str(self.server_metadata_attr))

        # exchange done, write message to buffer
        message = "a message from client"
        me_len = len(message)
        self.resource_send_mr.write(message, me_len)
        # cmid.post_write: need debian_v32
        self.cid.post_write(self.resource_send_mr, me_len,
                            self.server_metadata_attr.addr, self.server_metadata_attr.remote_stag)
        self.process_work_completion_events()
        resource_read_mr = MR(self.pd, c.BUFFER_SIZE,
                              e.IBV_ACCESS_LOCAL_WRITE | e.IBV_ACCESS_REMOTE_READ | e.IBV_ACCESS_REMOTE_WRITE)
        # cmid.post_read: need debian_v32
        self.cid.post_read(resource_read_mr, c.BUFFER_SIZE,
                           self.server_metadata_attr.addr, self.server_metadata_attr.remote_stag)
        self.process_work_completion_events()
        read_me = resource_read_mr.read(me_len, 0)
        print_info("read from the server buffer:\n"+str(read_me))
        return False

    def _on_disconnected(self) -> bool:
        self.cid.disconnect()
        self.close()
        return True

    # error: rejected
    def _on_rejected(self) -> bool:
        print("rejected?!")
        self.close()
        return True

    def close(self):
        self.cid.close()
        self.addr_info.close()
