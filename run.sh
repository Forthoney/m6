#!/bin/sh
ip=$(hostname -i)
while
	./distribution.js --ip "$ip" --port 7070 --local=2
do :; done
