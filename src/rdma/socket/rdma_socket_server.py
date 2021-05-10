# const
import pyverbs.cm_enums as ce
import pyverbs.enums as e
# config
import src.config.config as c
# pyverbs
from pyverbs.cmid import CMID, AddrInfo
from pyverbs.mr import MR
from pyverbs.pd import PD
from pyverbs.qp import QPInitAttr, QPCap


class RdmaSocketServer:
    def __init__(self, addr, port, options=c.OPTIONS):
        addr_info = AddrInfo(src=addr, src_service=port, port_space=ce.RDMA_PS_TCP, flags=ce.RAI_PASSIVE)
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
            while True:
                try:
                    new_id.accept()
                    print("a connect come")
                    pd = PD(new_id)
                    metadata_recv_mr = MR(pd, c.BUFFER_SIZE, e.IBV_ACCESS_LOCAL_WRITE)
                    new_id.post_recv(metadata_recv_mr)
                    
                except Exception as err:
                    print("error:", err)
                    break
            new_id.close()
