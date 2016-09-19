#!/usr/bin/env bash

IP=$1
NAME=$2
START_MODULE=""

if [ $3 = "master" ]; then
  START_MODULE="sodomongo start"
else
  START_MODULE="sodomongo start_deps"
fi

erl -name "${NAME}@${IP}" \
    -pa deps/bson/ebin \
    -pa deps/mongodb/ebin \
    -pa deps/pbkdf2/ebin/ \
    -pa deps/poolboy/ebin/ \
    -pa deps/bear/ebin/ \
    -pa deps/folsom/ebin/ \
    -pa deps/folsomite/ebin/ \
    -pa deps/zeta/ebin/ \
    -pa deps/protobuffs/ebin/ \
    -pa deps/sync/ebin/ \
    -pa deps/eredis/ebin \
    -pa ebin/ \
    -boot start_sasl \
    -config rel/sys \
    -s ${START_MODULE} \
    ${@:4}
