#!/usr/bin/env python3
import sys
import os
sys.path.extend([os.getcwd()])
from mapreduce.AReadUsers import GetUsers
print("init_reducer", file=sys.stderr)
reduce_object = GetUsers()
print("run_reducer", file=sys.stderr)
reduce_object.run()
print("end_reducer", file=sys.stderr)
