-module(sodomongo).

-export([
    start/0,
    start_deps/0,
    start_test/5,
    init_metrics/0,
    fill/1]).

-include("generator.hrl").

start() ->
    start_deps(),
    ok = application:start(sodomongo),
    cloud:start().

start_deps() ->
    ok = application:ensure_started(crypto),
    ok = application:ensure_started(bear),
    ok = application:ensure_started(folsom),
    ok = application:ensure_started(protobuffs),
    ok = application:ensure_started(inets),
    ok = application:ensure_started(compiler),
    ok = application:ensure_started(elixir),
    ok = application:ensure_started(eredis),
    ok = application:ensure_started(folsomite).

init_metrics() ->
    primary_inserter:init_metrics(),
    primary_reader:init_metrics(),
    ok.



fill(N) ->
    {ok, Conn} = aero:connect(application:get_all_env(sodomongo)),
    io:format("connect - done~n"),
    fill(Conn, 0, N). 

fill(Conn, N, N) ->
    aerospike:shutdown(Conn),
    ok;
fill(Conn, Idx, N) ->
    Data = shit_generator:gen(),
    io:format("data - done~n"),
    aerospike:put(Conn, "test", "data", Idx, [{"Bin", Data}], 0),
    io:format("~p~n", [N]),
    fill(Conn, Idx + 1, N).


start_test(_InsertWorkers, _UpdateWorkers, _DeleteWorkers, ReadWorkers, Time) ->
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


