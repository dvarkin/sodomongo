%%%-------------------------------------------------------------------
%%% @author serhiinechyporhuk
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 26. Aug 2016 13:54
%%%-------------------------------------------------------------------
-module(read_live_gameinfo_by_branch_task).
-author("serhiinechyporhuk").

%% API
-export([run/1]).

-include("profiler.hrl").

-define(TASK_SLEEP, 50).
-define(TASK, <<(atom_to_binary(?MODULE, utf8))/binary, "_metrics">>).
-define(RATE, <<?TASK/binary, ".rate">>).
-define(TIME, <<?TASK/binary, ".time">>).

get_gameinfo_ids(Connection) ->
    Cursor = mc_worker_api:find(
        Connection,
        <<"gameinfo">>,
        #{},
        #{projector => {<<"ID">>, true}}
    ),
    Result = mc_cursor:rest(Cursor),
    mc_cursor:close(Cursor),
    Result.

job(Connection, GameInfoIds) ->

    ?GPROF_TIME_METRIC(
        begin
            Cursor = mc_worker_api:find(
                Connection,
                <<"gameinfo">>,
                #{
                    <<"BranchId">> => #{ <<"$eq">> => util:rand_nth(GameInfoIds) }
                }
            ),
            mc_cursor:rest(Cursor)
        end,
        ?TIME),

    metrics:notify({?RATE, 1}),

    timer:sleep(?TASK_SLEEP),

    job(Connection, GameInfoIds).

run(Connection) ->
    GameInfoIds = get_gameinfo_ids(Connection),
    metrics:create(meter, ?RATE),
    metrics:create(histogram, ?TIME),
    job(Connection, GameInfoIds).