#!/usr/bin/env bash

ETH=$1
NAME=$2
START_MODULE=""

if [ $3 = "master" ]; then
  START_MODULE="sodomongo start"
else
  START_MODULE="sodomongo start_deps"
fi

IP=$(/sbin/ifconfig $ETH | grep 'inet addr:' | cut -d: -f2 | awk '{ print $1}')

Elixir=/usr/local/lib/elixir/lib/elixir/ebin/
DIR=`pwd`

erl -name "${NAME}@${IP}" \
    -args_file $DIR/rel/vm.args \
    -config $DIR/rel/sys \
    -env ELIXIR_EBIN $Elixir \
    -pa $Elixir \
    -pa $DIR/_build/dev/lib/bson/ebin \
    -pa $DIR/_build/dev/lib/mongodb/ebin \
    -pa $DIR/_build/dev/lib/pbkdf2/ebin \
    -pa $DIR/_build/dev/lib/poolboy/ebin\
    -pa $DIR/_build/dev/lib/bear/ebin\
    -pa $DIR/_build/dev/lib/folsom/ebin \
    -pa $DIR/_build/dev/lib/folsomite/ebin\
    -pa $DIR/_build/dev/lib/protobuffs/ebin\
    -pa $DIR/_build/dev/lib/sync/ebin\
    -pa $DIR/_build/dev/lib/sodomongo/ebin \
    -pa $DIR/_build/dev/lib/rethinkdb/ebin \
    -pa $DIR/_build/dev/lib/connection/ebin \
    -pa $DIR/_build/dev/lib/eredis/ebin \
    -s ${START_MODULE} \
    ${@:4}
