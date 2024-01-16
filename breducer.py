#!/usr/bin/env python3
import sys
import os
sys.path.extend([os.getcwd()])
from mapreduce.BGetConsumptions import GetConsumption
print("init_reducer", file=sys.stderr)
reduce_object = GetConsumption()
print("run_reducer", file=sys.stderr)
reduce_object.run()
print("end_reducer", file=sys.stderr)
