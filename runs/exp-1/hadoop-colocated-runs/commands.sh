#!/bin/bash
cat runs-*capacity*true*/logs_sum* | awk '{print $4}' > colocated-capacity-spec-true.txt 
cat runs-*capacity*false*/logs_sum* | awk '{print $4}' > colocated-capacity-spec-false.txt
cat runs-*fair*true*/logs_sum* | awk '{print $4 }' > colocated-fair-spec-true.txt
cat runs-*fair*false*/logs_sum* | awk '{print $4}' > colocated-fair-spec-false.txt



