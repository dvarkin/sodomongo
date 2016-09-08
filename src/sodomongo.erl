-module(sodomongo).

-export([start/0, start_deps/0, start_test/5, start_test/7, init_sharded_test/1]).

-include("generator.hrl").

start() ->
    start_deps(),
    ok = application:start(sodomongo),
    cloud:start().

start_deps() ->
    ok = application:ensure_started(crypto),
    ok = application:ensure_started(poolboy),
    ok = application:ensure_started(bson),
    ok = application:ensure_started(mongodb),
    ok = application:ensure_started(bear),
    ok = application:ensure_started(folsom),
    ok = application:ensure_started(protobuffs),
    ok = application:ensure_started(zeta),
    ok = application:ensure_started(folsomite).


init_sharded_test(Db) ->
    {ok, RawArgs} = application:get_env(sodomongo, mongo_connection),
    ConnectionArgs = lists:keyreplace(database, 1, RawArgs,{database, Db}),
    AdminConnectionArgs = lists:keyreplace(database, 1, ConnectionArgs, {database, <<"admin">>}),
    {ok, AdminConnection} = kinder:connect_to_mongo(AdminConnectionArgs),
    {ok, DbConnection} = kinder:connect_to_mongo(ConnectionArgs),

    {true, _} = mc_worker_api:command(DbConnection, #{<<"dropDatabase">> => 1}),
    {true, _} = mc_worker_api:command(AdminConnection, #{<<"enableSharding">> => ?DB}),
    case mc_worker_api:command(AdminConnection, #{<<"drop">> => ?GAMEINFO}) of
        {true, _} -> ok;
        {false, #{<<"code">> := 26}} -> ok
    end,
    case mc_worker_api:command(AdminConnection, #{<<"drop">> => ?MARKETINFO}) of
        {true, _} -> ok;
        {false, #{<<"code">> := 26}} -> ok
    end,
    {true, _} = mc_worker_api:command(AdminConnection, #{<<"create">> => ?GAMEINFO}),
    {true, _} = mc_worker_api:command(AdminConnection, #{<<"create">> => ?MARKETINFO}),

    mc_worker_api:ensure_index(DbConnection, ?MARKETINFO, #{<<"key">> => {?ID, <<"hashed">>}}),
    mc_worker_api:ensure_index(DbConnection, ?MARKETINFO, #{<<"key">> => {<<"Selections.ID">>, 1}}),
    mc_worker_api:ensure_index(DbConnection, ?GAMEINFO, #{<<"key">> => {?ID, <<"hashed">> }}),
    mc_worker_api:ensure_index(DbConnection, ?GAMEINFO, #{<<"key">> => {?BRANCH_ID, <<"hashed">> }}),

    {true, _} = mc_worker_api:command(AdminConnection, {
        <<"shardCollection">>, <<"test.",?GAMEINFO/binary>>,
        <<"key">>, {?ID, <<"hashed">>}
    }),
    {true, _} = mc_worker_api:command(AdminConnection, {
        <<"shardCollection">>, <<"test.",?MARKETINFO/binary>>,
        <<"key">>, {?ID, <<"hashed">>}
    }).

start_test(Db, InsertWorkers, UpdateWorkers, DeleteWorkers, ReadWorkers, ReadTaskModule, Time) ->
    init_sharded_test(Db),
    kinder_metrics:create_jobs([insert_gameinfo_task, update_odd_task, delete_gameinfo_task, ReadTaskModule]),
    hugin:start_job(insert_gameinfo_task, InsertWorkers, Time),
    hugin:start_job(update_odd_task, UpdateWorkers, Time),
    hugin:start_job(delete_gameinfo_task, DeleteWorkers, Time),
    hugin:start_job(ReadTaskModule, ReadWorkers, Time).

start_test(Db, InsertWorkers, UpdateWorkers, DeleteWorkers, Time) ->
    start_test(Db, InsertWorkers, UpdateWorkers, DeleteWorkers, 1, read_live_gameinfo_by_branch_task, Time).

