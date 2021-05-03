import pyverbs.enums as e

ADDR = "192.168.236.128"
PORT = "7471"
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
    }
}
