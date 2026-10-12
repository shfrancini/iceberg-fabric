#!/bin/bash

test "$1" = '-h' && { echo This script runs github ron003/detchannelmaps/ron/run_channel_map_api and more; exit; }

udppkt=0; \
for slot in 2 3 4;do\
    for stream in 0 1 2 3 64 65 66 67;do\
        test $slot = 4 -a $stream -gt 3 && continue;\
        for ch in `seq 0 63`;do\
	    xx=`run_channel_map_api --plugin ICEBERGChannelMap --crate 8 \
	    --slot $slot --stream $stream --chan $ch --plane`;\
	    echo  ochan/plane: $xx udppkt=$udppkt det=ICEBERG crate=8 slot=$slot stream=$stream strmchan=$ch;\
	done;\
        udppkt=$(($udppkt+1));\
    done;\
done | sort -k2n \
| while read lbl ochan rest;do
    channel-to-wire $ochan \
    | while read ln;do
        echo $ln plane=$rest
    done
done #| tee onchan-offchan-imagewire.txt

