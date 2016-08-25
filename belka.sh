#!/bin/sh

erl -sname 'belka' \
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
    -pa ebin/ \
    -config rel/sys \
    -setcookie abc \
    -s sodomongo start_deps 

