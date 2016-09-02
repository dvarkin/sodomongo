%%%-------------------------------------------------------------------
%%% @author eugeny
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 02. Sep 2016 10:30
%%%-------------------------------------------------------------------
-module(read_top_events_by_turnover_task).
-author("eugeny").

-include("generator.hrl").

%% API
-export([run/1, query/2]).

-define(TASK, <<(atom_to_binary(?MODULE, utf8))/binary, "_metrics">>).
-define(RATE, <<?TASK/binary, ".rate">>).
-define(TIME, <<?TASK/binary, ".time">>).
-define(DOC_COUNT, <<?TASK/binary, ".documents_count">>).
-define(OPERATIONS, <<?TASK/binary, ".operations">>).
-define(OPERATIONS_TOTAL, <<?OPERATIONS/binary, ".total">>).
-define(OPERATIONS_ERR, <<?OPERATIONS/binary, ".err">>).
-define(OPERATIONS_SUC, <<?OPERATIONS/binary, ".suc">>).

-define(QUERY_LIMIT, 10).
-define(TASK_SLEEP, 1).

get_branch_ids(Connection) ->
    Cursor = mc_worker_api:find(
        Connection,
        <<"gameinfo">>,
        #{},
        #{projector => {?BRANCH_ID, true}}
    ),

    Result = mc_cursor:rest(Cursor),
    BranchIDs = [BranchID || #{<<"BranchID">> := BranchID} <- Result],
    mc_cursor:close(Cursor),
    sets:to_list(sets:from_list(BranchIDs)).

query(Connection, BranchId) ->
    Command = {
        <<"aggregate">>, <<"gameinfo">>,
        <<"pipeline">>, [
            {<<"$match">>, {<<"BranchID">>, BranchId}},
            {<<"$sort">>, {<<"GameBets.TotalDepositGBP">>, -1}},
            {<<"$limit">>, ?QUERY_LIMIT}
        ]
    },
    {_, Result} = Response = profiler:prof(?TIME, fun() -> mc_worker_api:command(Connection, Command) end),

    case Response of
        {false, _} ->
            begin
                error_logger:error_msg("Can't read in module: ~p~n, response: ~p~n", [?MODULE, Response]),
                metrics:notify({?OPERATIONS_ERR, {inc, 1}})
            end;
        {true,  #{ <<"result">> := Res}} ->
            begin
                metrics:notify({?DOC_COUNT, length(Res)}),
                metrics:notify({?OPERATIONS_SUC, {inc, 1}})
            end
    end,

    metrics:notify({?RATE, 1}),
    metrics:notify({?OPERATIONS_TOTAL, {inc, 1}}),

    timer:sleep(?TASK_SLEEP),

    Result.

job(Connection, BranchId) ->
    query(Connection, BranchId),
    job(Connection, BranchId).

run(Connection) ->
    BranchIds = get_branch_ids(Connection),
    metrics:create(meter, ?RATE),
    metrics:create(histogram, ?TIME),
    metrics:create(histogram, ?DOC_COUNT),
    job(Connection, generator:rand_nth(BranchIds)).
