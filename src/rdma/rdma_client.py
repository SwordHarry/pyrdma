# rdma client
# const
import sys

import pyverbs.cm_enums as ce
import pyverbs.enums as e
# config
from pyverbs.wr import SGE, SendWR

import src.config.config as c
# common
from src.common.rdma_node import Node
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
        print("client has post_send metadata")
        self.process_work_completion_events()
        # get the server metadata attr
        self.server_metadata_attr = deserialize(self.metadata_recv_mr.read(c.BUFFER_SIZE, 0))
        print(self.server_metadata_attr)

        # exchange done, write message to buffer
        message = "a message from client"
        me_len = len(message)
        self.resource_mr.write(message, me_len)
        # sge = SGE(addr=self.resource_mr.buf, length=me_len, rkey=self.server_metadata_attr.remote_stag)
        # wr = SendWR(num_sge=1, sg=[sge], opcode=e.IBV_WR_RDMA_WRITE)
        # wr.set_wr_rdma(rkey=self.server_metadata_attr.remote_stag, addr=self.server_metadata_attr.addr)
        # self.qp.post_send(wr)
        self.cid.post_send(self.resource_mr)
        self.process_work_completion_events()
        return False

    def _on_disconnected(self) -> bool:
        self.cid.disconnect()
        self.close()
        return True

    # error: rejected
    def _on_rejected(self) -> bool:
        self.close()
        print("rejected?!")
        return True

    def close(self):
        self.cid.close()
        self.addr_info.close()
