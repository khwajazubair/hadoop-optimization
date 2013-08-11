#!/bin/bash
cat runs-*capacity*true*/logs_sum* | awk '{print $4}' > baseline_2-capacity-spec-true.txt 
cat runs-*capacity*false*/logs_sum* | awk '{print $4}' > baseline_2-capacity-spec-false.txt
cat runs-*fair*true*/logs_sum* | awk '{print $4}' > baseline_2-fair-spec-true.txt
cat runs-*fair*false*/logs_sum* | awk '{print $4}' > baseline_2-fair-spec-false.txt



