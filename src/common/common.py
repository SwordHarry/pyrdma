# request struct and config
import threading


def die(reason):
    print(reason)
    exit(1)


class PollThread(threading.Thread):
    def __init__(self, task=None, thread_id=1):
        threading.Thread.__init__(self)
        self.threadID = thread_id
        self.task = task

    def run(self):
        if self.task is not None:
            self.task()
