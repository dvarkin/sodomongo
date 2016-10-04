-module(sodomongo).

-export([
    start/0,
    start_deps/0,
    start_test/5,
    init_metrics/0
    , create_tables/0]).

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
    ok = application:ensure_started(inets),
    ok = application:ensure_started(compiler),
    ok = application:ensure_started(elixir),
    ok = application:ensure_started(eredis),
    %ok = application:ensure_started(zeta),
    ok = application:ensure_started(folsomite).

init_metrics() ->
%%    insert_gameinfo_task:init_metrics(),
%%    insert_marketinfo_task:init_metrics(),
%%    update_odd_task:init_metrics(),
%%    delete_gameinfo_task:init_metrics(),
%%    delete_marketinfo_task:init_metrics(),
%%    read_branches_with_active_games_worker:init_metrics(),
%%    read_leagues_by_branch_with_number_of_games_in_active_state_worker:init_metrics(),
%%    read_live_gameinfo_by_branch_worker:init_metrics(),
%%    read_non_zero_odds_markets_worker:init_metrics(),
%%    read_top_events_by_turnover_worker:init_metrics(),
%%    read_top_events_starting_soon_worker:init_metrics(),
    ok.



create_table(TableName, Conn) ->
    #{data := TableList} = rethinkdb:r([[table_list]], Conn),
    case lists:member(TableName, TableList) of
        true -> rethinkdb:r([[table_drop, TableName]], Conn);
        _ -> ok
    end,
    rethinkdb:r([[table_create, TableName, #{shards => 5, replicas => 2}]], Conn).

create_tables() ->
    {ok, Conn} = rethinkdb:connect(application:get_all_env(sodomongo)),
    create_table(<<"gameinfo">>, Conn),
    create_table(<<"marketinfo">>, Conn),
    ok.

start_test(_InsertWorkers, _UpdateWorkers, _DeleteWorkers, ReadWorkers, Time) ->
    create_tables(),
%%    init_replica_test(),
%%    %init_metrics(),
%%    meta_storage:flush(),
%%    hugin:start_job(insert_gameinfo_task, InsertWorkers, Time, 1),
%%    hugin:start_job(insert_marketinfo_task, InsertWorkers, Time, 1),
%%
%%    hugin:start_job(update_odd_task, UpdateWorkers, Time, 1),
%%
%%    hugin:start_job(delete_gameinfo_task, DeleteWorkers, Time, 1),
%%    hugin:start_job(delete_marketinfo_task, DeleteWorkers, Time, 1),
%%
%%    hugin:start_job(read_branches_with_active_games_worker, ReadWorkers, Time, 1),
%%    hugin:start_job(read_leagues_by_branch_with_number_of_games_in_active_state_worker, ReadWorkers, Time, 1),
%%    hugin:start_job(read_live_gameinfo_by_branch_worker, ReadWorkers, Time, 1),
%%    hugin:start_job(read_non_zero_odds_markets_worker, ReadWorkers, Time, 1),
%%
%%    hugin:start_job(read_top_events_by_turnover_worker, ReadWorkers, Time, 100),
%%    hugin:start_job(read_top_events_starting_soon_worker, ReadWorkers, Time, 100),
    hugin:start_job(test_read, ReadWorkers, Time, 100),
    ok.


