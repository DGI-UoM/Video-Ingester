#!/bin/bash

# script is running as root
if [ $EUID -ne 0 ]; then
    echo "This script requires root access to run"
else
    python Ingester.py > >(tee stdout.log) 2>stderr.log
fi
