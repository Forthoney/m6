#!/bin/sh
# Splits bigurls.txt to smaller batches.
# Output format will be output_<n>
split -d -n 100 bigurls.txt output_
