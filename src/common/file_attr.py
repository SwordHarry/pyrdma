
class FileAttr:
    def __init__(self, file_path=""):
        self.file_name = file_path
        self.fd = None
        self.file_done = False

    def open(self, file_path):
        if not self.fd:
            self.fd = open(file_path, "rb")

    def is_done(self):
        return self.file_done

    def done(self):
        self.file_done = True

    def close(self):
        if self.fd:
            self.fd.close()
            self.fd = None
        self.file_name = ""
        self.file_done = False

    def __str__(self):
        return "===============\n" \
               "file attr:\n file_name: %s;\n fd: %s;\n file_done: %s;\n" \
               "===============\n" % (self.file_name, self.fd, self.file_done)
