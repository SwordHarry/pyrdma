import pyverbs.enums as e

ADDR = "192.168.236.128"
PORT_INT = 50008
PORT_STR = "50008"
NAME = "rxe_0"
TIMEOUT_IN_MS = 500
BUFFER_SIZE = 1024

OPTIONS = {
    "qp_init": {
        "qp_type": e.IBV_QPT_RC,
        "max_send_wr": 4,
        "max_recv_wr": 4,
        "max_send_sge": 2,
        "max_recv_sge": 2,
    },
    "cq_init": {
        "cqe": 8
    },
    "gid_init": {
        "port_num": 1,
        "gid_index": 1,
    }
}
