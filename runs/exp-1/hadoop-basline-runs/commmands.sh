#!/bin/bash
cat runs-*capacity*true*-1375729570/logs_sum* | awk '{print $4}' > bs_ct_1.csv 
cat runs-*capacity*false*/logs_sum* | awk '{print $4, $15}' > baseline-capacity-spec-false.txt
cat runs-*fair*true*-1375749087/logs_sum* | awk '{print $4}' > bs_ft_1.csv
cat runs-*fair*false*/logs_sum* | awk '{print $4, $15}' > baseline-fair-spec-false.txt



