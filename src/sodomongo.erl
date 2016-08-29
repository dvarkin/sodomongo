-module(sodomongo).

-export([start/0, start_deps/0, start_test/4, start_test/6, init_test/0]).

start() ->
    start_deps(),
    ok = application:start(sodomongo),
    net_adm:world().

start_deps() ->
    ok = application:ensure_started(crypto),
    ok = application:ensure_started(poolboy),
    ok = application:ensure_started(bson),
    ok = application:ensure_started(mongodb),
    ok = application:ensure_started(bear),
    ok = application:ensure_started(folsom),
    ok = application:ensure_started(protobuffs),
    ok = application:ensure_started(zeta),
    ok = application:ensure_started(folsomite),
    ok = application:ensure_started(compiler),
    ok = application:ensure_started(syntax_tools).


init_test() ->
    {ok, ConnectionArgs} = application:get_env(sodomongo, mongo_connection),
    {ok, Connection} = kinder:connect_to_mongo(ConnectionArgs),
    init_test:run(Connection).


start_test(InsertWorkers, UpdateWorkers, DeleteWorkers, ReadWorkers, ReadTaskModule, Time) ->
    init_test(),
    hugin:start_job(insert_gameinfo_task, InsertWorkers, Time),
    hugin:start_job(update_odd_task, UpdateWorkers, Time),
    hugin:start_job(delete_gameinfo_task, DeleteWorkers, Time),
    hugin:start_job(ReadTaskModule, ReadWorkers, Time).


start_test(InsertWorkers, UpdateWorkers, DeleteWorkers, Time) ->
    start_test(InsertWorkers, UpdateWorkers, DeleteWorkers, 1, read_live_gameinfo_by_branch_task, Time).

