#!/bin/zsh

for file in jstests/*.js
do
	echo "running $file"
	mongo --quiet 127.0.0.1:27017/test $file || exit
done

echo "successfully finished"
