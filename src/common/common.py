# request struct and config
from pyverbs.cq import CQ
import threading


def die(reason):
    print(reason)
    exit(1)


class PollThread(threading.Thread):
    def __init__(self, context, thread_id=1, on_completion=None):
        threading.Thread.__init__(self)
        self.context = context
        self.threadID = thread_id
        self.on_completion = on_completion

    def run(self):
        try:
            self._poll_cq(self.context)
        except Exception as e:
            print(e)

    # TODO: complete the poll
    def _poll_cq(self, context):
        while True:
            # TODO: what is the ctx? None?
            cq = CQ(context.ctx, 10, None, context.comp_channel, 0)
            context.comp_channel.get_cq_event(cq)
            cq.ack_events(1)
            cq.req_notify()
            (npolled, wcs) = cq.poll(1)
            print("npolled:", npolled)
            if self.on_completion is not None:
                self.on_completion(wcs[0])
