#!/bin/bash

udppkt=0 

for slot in 2 3 4; do
    for stream in 0 1 2 3 64 65 66 67; do
        [[ $slot == 4 && $stream -gt 3 ]] && continue
        for ch in $(seq 0 63); do
            adc_idx=$((ch/16))
            adc_chan_idx=$((ch-(adc_idx*16)))
            xx=$(run_channel_map_api --plugin ICEBERGChannelMap --crate 8 \
            --slot $slot --stream $stream --chan $ch --plane)
            echo "ochan/plane: $xx udppkt=$udppkt det=ICEBERG crate=8 slot=$slot stream=$stream strmchan=$ch adc_idx=$adc_idx adc_chan_idx=$adc_chan_idx"
        done
        ((udppkt++))
    done
done
