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
    -pa _build/dev/lib/bson/ebin \
    -pa _build/dev/lib/mongodb/ebin \
    -pa _build/dev/lib/pbkdf2/ebin/ \
    -pa _build/dev/lib/poolboy/ebin/ \
    -pa _build/dev/lib/bear/ebin/ \
    -pa _build/dev/lib/folsom/ebin/ \
    -pa _build/dev/lib/folsomite/ebin/ \
    -pa _build/dev/lib/protobuffs/ebin/ \
    -pa _build/dev/lib/sync/ebin/ \
    -pa _build/dev/lib/sodomongo/ebin \
    -pa _build/dev/lib/rethinkdb/ebin \
    -pa _build/dev/lib/connection/ebin \
    -pa _build/dev/lib/eredis/ebin \
    -pa $ELIXIR_EBIN/ \
    -boot start_sasl \
    -kernel error_logger silent \
    -config rel/sys \
    -s ${START_MODULE} \
    ${@:4}
