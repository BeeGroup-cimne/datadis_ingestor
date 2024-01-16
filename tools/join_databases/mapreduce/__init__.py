import sys


class Map(object):
    input_file = sys.stdin
    SEP = "\t"

    @staticmethod
    def read_input(file):
        for line in file:
            yield line.strip()

    def finish(self):
        print("finish", file=sys.stderr)

    def run(self):
        print("starting", file=sys.stderr)
        for line in Map.read_input(Map.input_file):
            self.map(line)
        print(f"finished", file=sys.stderr)
        self.finish()

    def map(self, line):
        raise NotImplemented


class Reduce(object):
    input_file = sys.stdin
    SEP = "\t"

    @staticmethod
    def read_input(file):
        prev_key = None
        values = []
        print("debugging", file=sys.stderr)
        for line in file:
            if isinstance(line, str):
                k, v = line.split(Reduce.SEP)
            else:
                print(str(type(line)), file=sys.stderr)
            if prev_key is None:
                prev_key = k
                values.append(v)
            elif prev_key == k:
                values.append(v)
            elif prev_key != k:
                yield prev_key, values
                prev_key = k
                values = [v]
        if prev_key:
            yield prev_key, values

    def finish(self):
        print("finish", file=sys.stderr)

    def run(self):
        print("starting", file=sys.stderr)
        for k, v in Reduce.read_input(Reduce.input_file):
            self.reduce(k, v)
        print(f"finished", file=sys.stderr)
        self.finish()

    def reduce(self, k, v):
        raise NotImplemented
