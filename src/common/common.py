# request struct and config
from pyverbs.cq import CQ
import threading


def die(reason):
    print(reason)
    exit(1)


def print_info(text=""):
    print("====================================")
    print(text)
    print("====================================")


class PollThread(threading.Thread):
    def __init__(self, context, thread_id=1, on_completion=None):
        threading.Thread.__init__(self)
        self.context = context
        self.threadID = thread_id
        self.on_completion = on_completion

    def run(self):
        pass
