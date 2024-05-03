#!/bin/sh

j=0; while [ $j -le 10 ]; do
	i=10; while [ $i -le 200 ]; do
		./distribution.js --spawner=$i | grep duration | awk '{print $2}' >> spawner-results-$j.txt
		i=$(( i + 10 ))
	done
	j=$(( j + 1 ))
done
