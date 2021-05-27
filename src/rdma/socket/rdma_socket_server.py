# const
import pickle

import pyverbs.cm_enums as ce
import pyverbs.enums as e
# config
import src.config.config as c
# common
from src.common.buffer_attr import BufferAttr, deserialize, serialize
from src.common.common import check_wc_status, print_info
# pyverbs: need v32
from pyverbs.cmid import CMID, AddrInfo
from pyverbs.mr import MR
from pyverbs.pd import PD
from pyverbs.qp import QPInitAttr, QPCap
# util
from src.rdma.socket.rdma_socket_util import process_wc_send_events, process_wc_recv_events


class RdmaSocketServer:
    def __init__(self, addr, port, options=c.OPTIONS):
        addr_info = AddrInfo(src=addr, src_service=port, port_space=ce.RDMA_PS_TCP, flags=ce.RAI_PASSIVE)# v32
        print(addr, port)
        # addr_info = AddrInfo(src=addr, service=port, port_space=ce.RDMA_PS_TCP, flags=ce.RAI_PASSIVE)
        qp_options = options["qp_init"]
        cap = QPCap(max_send_wr=qp_options["max_send_wr"], max_recv_wr=qp_options["max_recv_wr"],
                    max_send_sge=qp_options["max_send_sge"], max_recv_sge=qp_options["max_recv_sge"])
        qp_init_attr = QPInitAttr(qp_type=qp_options["qp_type"], cap=cap)
        self.sid = CMID(creator=addr_info, qp_init_attr=qp_init_attr)

    def serve(self):
        self.sid.listen()
        print("rdma socket listening")
        while True:
            new_id = self.sid.get_request()
            new_id.accept()
            print("\n---------------------------- A CONNECT ACCEPT  --------------------------------")
            while True:
                try:
                    pd = PD(new_id)
                    metadata_recv_mr = MR(pd, c.BUFFER_SIZE, e.IBV_ACCESS_LOCAL_WRITE)
                    new_id.post_recv(metadata_recv_mr)
                    process_wc_recv_events(new_id)
                    client_metadata_attr = deserialize(metadata_recv_mr.read(c.BUFFER_SIZE, 0))
                    print_info("client metadata attr:\n" + str(client_metadata_attr))
                    resource_send_mr = MR(pd, c.BUFFER_SIZE,
                                          e.IBV_ACCESS_LOCAL_WRITE |
                                          e.IBV_ACCESS_REMOTE_READ |
                                          e.IBV_ACCESS_REMOTE_WRITE)
                    # metadata_send_mr: client send the resource_send_mr attr to server
                    buffer_attr = BufferAttr(addr=resource_send_mr.buf, length=c.BUFFER_SIZE,
                                             local_stag=resource_send_mr.lkey,
                                             remote_stag=resource_send_mr.rkey)
                    buffer_attr_bytes = serialize(buffer_attr)
                    # metadata_send_mr: node send the resource_send_mr attr to others
                    metadata_send_mr = MR(pd, c.BUFFER_SIZE, e.IBV_ACCESS_LOCAL_WRITE)
                    metadata_send_mr.write(buffer_attr_bytes, len(buffer_attr_bytes))

                    new_id.post_send(metadata_send_mr)
                    process_wc_send_events(new_id)
                    # wait for done
                    new_id.post_recv(metadata_recv_mr)
                    process_wc_recv_events(new_id)
                    done_message = metadata_recv_mr.read(c.BUFFER_SIZE, 0)
                    if done_message == "done":
                        new_id.close()
                        print("---------------------------- A CONNECT DONE  --------------------------------")
                        break
                except Exception as err:
                    print("error:", err)
                    break
                finally:
                    new_id.close()
                    print("---------------------------- A CONNECT DONE  --------------------------------")
                    break
