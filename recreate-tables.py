#!/usr/bin/env python

import subprocess
import time

subprocess.call("./delete-tables.sh")
time.sleep(0.5)
subprocess.call("./create-tables.sh")
