from pyverbs.enums import IBV_QPT_RC

ADDR = "192.168.236.128"
PORT = "7471"
NAME = "rxe_0"
TIMEOUT_IN_MS = 500

OPTIONS = {
    "qp_init": {
        "qp_type": IBV_QPT_RC,
        "max_send_wr": 10,
        "max_recv_wr": 10,
        "max_send_sge": 1,
        "max_recv_sge": 1,
    }
}
