#!/bin/bash

python Ingester.py > >(tee stdout.log) 2> >(tee stderr.log >&2)
