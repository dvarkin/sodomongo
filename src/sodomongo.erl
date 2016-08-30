-module(sodomongo).

-export([start/0, start_deps/0, start_test/4, start_test/6, init_test/0]).

-include("generator.hrl").

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
    AdminConnectionArgs = lists:keyreplace(database, 1, ConnectionArgs, {database, <<"admin">>}),
    {ok, AdminConnection} = kinder:connect_to_mongo(AdminConnectionArgs),
    {ok, DbConnection} = kinder:connect_to_mongo(ConnectionArgs),

    {true, _} = mc_worker_api:command(DbConnection, #{<<"dropDatabase">> => 1}),
    {true, _} = mc_worker_api:command(AdminConnection, #{<<"enableSharding">> => ?DB}),
    {true, _} = mc_worker_api:command(AdminConnection, #{<<"drop">> => ?GAMEINFO}),
    {true, _} = mc_worker_api:command(AdminConnection, #{<<"drop">> => ?MARKETINFO}),
    {true, _} = mc_worker_api:command(AdminConnection, #{<<"create">> => ?GAMEINFO}),
    {true, _} = mc_worker_api:command(AdminConnection, #{<<"create">> => ?MARKETINFO}),

    mc_worker_api:ensure_index(AdminConnection, ?MARKETINFO, #{<<"key">> => {?ID, <<"hashed">>}}),
    mc_worker_api:ensure_index(AdminConnection, ?MARKETINFO, #{<<"key">> => {<<"Selections.ID">>, 1}}),
    mc_worker_api:ensure_index(AdminConnection, ?GAMEINFO, #{<<"key">> => {?GAME_ID, <<"hashed">> }}),
    mc_worker_api:ensure_index(AdminConnection, ?GAMEINFO, #{<<"key">> => {?BRANCH_ID, <<"hashed">> }}),

    {true, _} = mc_worker_api:command(AdminConnection, {
        <<"shardCollection">>, ?GAMEINFO,
        <<"key">>, {?GAME_ID, <<"hashed">>}
    }),
    {true, _} = mc_worker_api:command(AdminConnection, {
        <<"shardCollection">>, ?MARKETINFO,
        <<"key">>, {?ID, <<"hashed">>}
    }).


start_test(InsertWorkers, UpdateWorkers, DeleteWorkers, ReadWorkers, ReadTaskModule, Time) ->
    init_test(),
    kinder_metrics:create_jobs([insert_gameinfo_task, update_odd_task, delete_gameinfo_task, ReadTaskModule]),
    hugin:start_job(insert_gameinfo_task, InsertWorkers, Time),
    hugin:start_job(update_odd_task, UpdateWorkers, Time),
    hugin:start_job(delete_gameinfo_task, DeleteWorkers, Time),
    hugin:start_job(ReadTaskModule, ReadWorkers, Time).

start_test(InsertWorkers, UpdateWorkers, DeleteWorkers, Time) ->
    start_test(InsertWorkers, UpdateWorkers, DeleteWorkers, 1, read_live_gameinfo_by_branch_task, Time).

