#!/usr/bin/env python3
import sys
import os
sys.path.extend([os.getcwd()])
from mapreduce.AReadUsers import ReadUsers
print("init_map", file=sys.stderr)
mapper_object = ReadUsers()
print("run_map", file=sys.stderr)
mapper_object.run()
print("end_map", file=sys.stderr)
