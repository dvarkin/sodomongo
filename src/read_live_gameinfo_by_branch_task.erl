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

-define(TASK_SLEEP, 50).
-define(TASK, <<(atom_to_binary(?MODULE, utf8))/binary, "_metrics">>).
-define(RATE, <<?TASK/binary, ".rate">>).
-define(TIME, <<?TASK/binary, ".time">>).

create_indexes(Connection) ->
    mc_worker_api:ensure_index(Connection, <<"gameinfo">>, #{<<"key">> => {<<"BranchID">>, <<"hashed">>}}).

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

    {Time, _Value} = timer:tc(
        fun() ->
            Cursor = mc_worker_api:find(
                Connection,
                <<"gameinfo">>,
                #{
                    <<"BranchId">> => #{ <<"$eq">> => util:rand_nth(GameInfoIds) }
                }
            ),
            mc_cursor:rest(Cursor)
        end),

    metrics:notify({?RATE, 1}),
    metrics:notify({?TIME, Time}),

    timer:sleep(?TASK_SLEEP),

    job(Connection, GameInfoIds).

run(Connection) ->
    create_indexes(Connection),
    GameInfoIds = get_gameinfo_ids(Connection),
    metrics:create(meter, ?RATE),
    metrics:create(gauge, ?TIME),
    job(Connection, GameInfoIds).