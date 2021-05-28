
class FileAttr:
    def __init__(self, file_path):
        self.file_name = file_path
        self.fd = None
        self.file_done = False

    def close(self):
        self.fd.close()
        self.file_name = ""
        self.file_done = False
