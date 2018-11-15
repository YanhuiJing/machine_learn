#! /bin/bash
rm -rf ./dataSource/*

count=0

while [ $count -le 5 ]; do
    cp result.txt "$count"
    mv "$count" ./dataSource
    count=$((count+1))
    sleep 3s

done

