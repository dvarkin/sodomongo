-module(sodomongo).

-export([start/0, start_deps/0, start_test/5, init_sharded_test/1, init_replica_test/0]).

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
    kinder_mertics:init(),
    timer:apply_interval(30000, kinder_metrics, nofity, []),

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

init_replica_test() ->
    RawArgs = mongo:conn_args(),
    
    {ok, DbConnection} = mongo:connect_to_master(RawArgs),
    AdminConnection = DbConnection,
%    {ok, AdminConnection} = mongo:connect_to_master(AdminConnectionArgs),

    {true, _} = mc_worker_api:command(DbConnection, #{<<"dropDatabase">> => 1}),
    case mc_worker_api:command(AdminConnection, #{<<"drop">> => ?GAMEINFO}) of
        {true, _} -> ok;
        {false, #{<<"code">> := 26}} -> ok
    end,
    case mc_worker_api:command(AdminConnection, #{<<"drop">> => ?MARKETINFO}) of
        {true, _} -> ok;
        {false, #{<<"code">> := 26}} -> ok
    end,
    case mc_worker_api:command(AdminConnection, #{<<"create">> => ?GAMEINFO}) of
        {true, _} -> ok;
        {false, #{<<"code">> := 48}} -> ok
    end,
    case mc_worker_api:command(AdminConnection, #{<<"create">> => ?MARKETINFO}) of
        {true, _} -> ok;
        {false, #{<<"code">> := 48}} -> ok
    end,
    mc_worker_api:ensure_index(DbConnection, ?MARKETINFO, #{<<"key">> => {?ID, <<"hashed">>}}),
    mc_worker_api:ensure_index(DbConnection, ?MARKETINFO, #{<<"key">> => {<<"Selections.ID">>, 1}}),
    mc_worker_api:ensure_index(DbConnection, ?GAMEINFO, #{<<"key">> => {?ID, <<"hashed">> }}),
    mc_worker_api:ensure_index(DbConnection, ?GAMEINFO, #{<<"key">> => {?BRANCH_ID, <<"hashed">> }}).

init_metrics() ->
    insert_gameinfo_task:init_metrics(),
    insert_marketinfo_task:init_metrics(),
    update_odd_task:init_metrics(),
    delete_gameinfo_task:init_metrics(),
    delete_marketinfo_task:init_metrics(),
    read_branches_with_active_games_worker:init_metrics(),
    read_leagues_by_branch_with_number_of_games_in_active_state_worker:init_metrics(),
    %% read_live_gameinfo_by_branch_worker:init_metrics(),
    %% read_non_zero_odds_markets_worker:init_metrics(),
    %% read_top_events_by_turnover_worker:init_metrics(),
    %% read_top_events_starting_soon_worker:init_metrics(),
    ok.

start_test(InsertWorkers, UpdateWorkers, DeleteWorkers, ReadWorkers, Time) ->
    init_replica_test(),
    init_metrics(),
    meta_storage:flush(),
    hugin:start_job(insert_gameinfo_task, InsertWorkers, Time, 100),
    hugin:start_job(insert_marketinfo_task, InsertWorkers, Time, 100),

    hugin:start_job(update_odd_task, UpdateWorkers, Time, 100),

    hugin:start_job(delete_gameinfo_task, DeleteWorkers, Time, 1000),
    hugin:start_job(delete_marketinfo_task, DeleteWorkers, Time, 100),

    hugin:start_job(read_branches_with_active_games_worker, ReadWorkers, Time, 500),
    hugin:start_job(read_leagues_by_branch_with_number_of_games_in_active_state_worker, ReadWorkers, Time, 100),
    %% hugin:start_job(read_live_gameinfo_by_branch_worker, ReadWorkers, Time, 100),
    %% hugin:start_job(read_non_zero_odds_markets_worker, ReadWorkers, Time, 100),
    %% hugin:start_job(read_top_events_by_turnover_worker, ReadWorkers, Time, 100),
    %% hugin:start_job(read_top_events_starting_soon_worker, ReadWorkers, Time, 100),

    ok.
    
    
    %% kinder_metrics:create_jobs([insert_gameinfo_task, update_odd_task, delete_gameinfo_task, ReadTaskModule]),
    %% hugin:start_job(insert_gameinfo_task, InsertWorkers, Time),
    %% hugin:start_job(update_odd_task, UpdateWorkers, Time),
    %% hugin:start_job(delete_gameinfo_task, DeleteWorkers, Time),
    %% hugin:start_job(ReadTaskModule, ReadWorkers, Time).

%% start_test(Db, InsertWorkers, UpdateWorkers, DeleteWorkers, Time) ->
%%     start_test(Db, InsertWorkers, UpdateWorkers, DeleteWorkers, 1, read_live_gameinfo_by_branch_task, Time).



%% test_c(Collection, Query) ->
%%     ConnectionArgs = mongo:conn_args(),
%%     error_logger:info_msg("Args: ~p", [ConnectionArgs]),
%%     {ok, MasterConn} = mongo:connect_to_master(ConnectionArgs),
%%     {ok, SecConn} = mongo:connect_to_secondary(ConnectionArgs),
%%     MR = mc_worker_api:find_one(MasterConn, Collection, Query),
%%     SR = mc_worker_api:find_one(SecConn, Collection, Query),
%%     {MR, SR}.
    
    
