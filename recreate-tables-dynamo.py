#!/usr/bin/env python

import subprocess
import time

subprocess.call("./delete-tables-dynamo.sh")
time.sleep(0.5)
subprocess.call("./create-tables-dynamo.sh")
