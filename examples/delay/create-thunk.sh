#!/bin/bash -e

USAGE="$0 <DELAY-PATH> <NAME> <T> <thunk-hash/thunk-placeholder>"

DELAY_PATH=${1?$USAGE}
NAME=${2?$USAGE}
T=${3?$USAGE}
THUNK_PATH=${4?$USAGE}

DELAY_HASH=$(gg-hash $DELAY_PATH)

gg-create-thunk --local \
                --envar GG_DIR=${GG_DIR} \
                --envar GG_MODELPATH=${GG_MODELPATH} \
                --executable ${DELAY_HASH} \
                --output out \
                --placeholder delay${T}_${NAME} \
                ${DELAY_HASH} delay ${T} ${THUNK_PATH}

gg-collect $DELAY_PATH