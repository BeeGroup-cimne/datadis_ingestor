#!/usr/bin/env python3
import sys
import os
sys.path.extend([os.getcwd()])
from mapreduce.BGetConsumptions import ReadSupplies
print("init_map", file=sys.stderr)
mapper_object = ReadSupplies()
print("run_map", file=sys.stderr)
mapper_object.run()
print("end_map", file=sys.stderr)
