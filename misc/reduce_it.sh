#!/bin/bash

docker run -it --rm \
-v $PWD/data:/app/inbound \
-v $PWD/data:/app/outbound \
--env REDUCER_RUNNERS_QTY=4 \
--env REDUCER_HIGH_RUNNER=4 \
 reducer