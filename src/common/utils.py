# request struct and config
import os

import pyverbs.cq
import pyverbs.enums as e


def check_msg(msg, msg2):
    return msg.decode("UTF-8", "ignore").strip("\x00").encode() == msg2


def die(reason):
    print(reason)
    exit(1)


def print_info(text=""):
    print("===============================================================")
    print(text)
    print("===============================================================")


def check_wc_status(wc: pyverbs.cq.WC):
    if wc.status != e.IBV_WC_SUCCESS:
        print(wc)
        die("on_completion: status is not IBV_WC_SUCCESS")
    if wc.opcode & e.IBV_WC_RECV:
        print("received message")
    elif wc.opcode == e.IBV_WC_SEND:
        print("send completed successfully")
    elif wc.opcode == e.IBV_WC_RDMA_WRITE:
        print("write complete")
    elif wc.opcode == e.IBV_WC_RECV_RDMA_WITH_IMM:
        print("write with imm_data:", wc.imm_data)
    elif wc.opcode == e.IBV_WC_RDMA_READ:
        print("read complete")
    else:
        die("completion isn't a send, write, read or a receive")


def create_file(file_name):
    dirname = os.path.dirname(file_name)
    if not os.path.isdir(dirname):
        os.makedirs(dirname)
    return open(file_name, "wb+")
