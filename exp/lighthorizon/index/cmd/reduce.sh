#!/bin/bash

# check parameters and their validity (types, existence, etc.)

if [[ "$#" -ne "2" ]]; then 
    echo "Usage: $0 <index src root> <index dest>"
    exit 1
fi

if [[ ! -d "$1" ]]; then 
    echo "Error: index src root ('$1') does not exist"
    echo "Usage: $0 <index src root> <index dest>"
    exit 1
fi

if [[ ! -d "$2" ]]; then 
    echo "Warning: index dest ('$2') does not exist, creating..."
    mkdir -p $2
fi


go build -o reduce ./batch/reduce/...
if [[ "$?" -ne "0" ]]; then 
    echo "Build failed"
    exit 1
fi

JOB_COUNT=$(ls $1 | grep -E 'job_[0-9]+' | wc -l)
if [[ "$JOB_COUNT" -le "0" ]]; then 
    echo "No jobs in index src root ('$1') found."
    exit 1 
fi

pids=( )
for (( i=0; i < $JOB_COUNT; i++ ))
do
    echo -n "Creating job $i... "

    AWS_BATCH_JOB_ARRAY_INDEX=$i MAP_JOB_COUNT=$JOB_COUNT \
    REDUCE_JOB_COUNT=$JOB_COUNT WORKER_COUNT=4 \
    INDEX_SOURCE_ROOT=file://$1 INDEX_TARGET=file://$2 \
        ./reduce &
    
    echo "pid=$!"
    pids+=($!)
done

sleep $JOB_COUNT

# Check the status codes for all of the map processes.
for i in "${!pids[@]}"; do
    pid=${pids[$i]}
    echo -n "Checking job $i (pid=$pid)... "
    if ! wait "$pid"; then
        echo "failed"
        exit 1
    else
        echo "succeeded!"
    fi
done

rm ./reduce
echo "All jobs succeeded!"
exit 0