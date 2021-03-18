class Properties(object):
    def __init__(self, file_path=None):
        self.ready = True
        self.err = ''
        self.d = {}
        if file_path is None:
            return
        try:
            f = open(file_path)
            for line in f:
                line = line.rstrip()
                if "#" in line or "=" not in line:
                    continue
                loc = line.find("=")
                if 0 < loc < len(line) - 1:
                    key = line[0:loc]
                    val = line[loc + 1:]
                    self.d[key] = val
            f.close()
        except IOError as ioe:
            self.err = "%s" % ioe
            self.ready = False

    def get_value(self, key, default_value):
        if key in self.d:
            return self.d[key]
        else:
            return default_value

    def set_value(self, key, value):
        self.d[key] = value

    def is_ready(self):
        return self.ready

    def get_last_error(self):
        return self.err
