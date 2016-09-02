%%%-------------------------------------------------------------------
%%% @author serhiinechyporhuk
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 01. Sep 2016 15:43
%%%-------------------------------------------------------------------
-module(read_leagues_by_branch_with_number_of_games_in_active_state).
-author("serhiinechyporhuk").

%% API
-export([run/1]).

-include("generator.hrl").

-define(TASK_SLEEP, 100).
-define(TASK, <<(atom_to_binary(?MODULE, utf8))/binary, "_metrics">>).
-define(RATE, <<?TASK/binary, ".rate">>).
-define(TIME, <<?TASK/binary, ".time">>).
-define(DOC_COUNT, <<?TASK/binary, ".documents_count">>).
-define(OPERATIONS, <<?TASK/binary, ".operations">>).
-define(OPERATIONS_TOTAL, <<?OPERATIONS/binary, ".total">>).
-define(OPERATIONS_ERR, <<?OPERATIONS/binary, ".err">>).
-define(OPERATIONS_SUC, <<?OPERATIONS/binary, ".suc">>).

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

job(Connection, BranchIDs) ->
    BranchID = util:rand_nth(BranchIDs),
    IsLive = util:rand_nth([true, false]),
    Command = {
        <<"aggregate">>, <<"gameinfo">>,
        <<"pipeline">>, [
            {
                <<"$match">>,
                {
                    <<"BranchID">>, BranchID,
                    <<"IsLive">>, IsLive,
                    <<"IsActive">>, true
                }
            },
            {
                <<"$group">>,
                {
                    <<"_id">>, <<"$LeagueID">>,
                    <<"ActiveEvents">>, {<<"$sum">>, 1}
                }
            }
        ]
    },
    Response = profiler:prof(?TIME, fun() -> mc_worker_api:command(Connection, Command) end),
    io:format("~p~n", [Response]),
    case Response of
        {false, _} ->
            begin
                error_logger:error_msg("Can't read in module: ~p~n, response: ~p~n", [?MODULE, Response]),
                metrics:notify({?OPERATIONS_ERR, {inc, 1}})
            end;
        {true,  #{ <<"result">> := Result }} ->
            begin
                metrics:notify({?DOC_COUNT, length(Result)}),
                metrics:notify({?OPERATIONS_SUC, {inc, 1}})
            end
    end,

    metrics:notify({?RATE, 1}),
    metrics:notify({?OPERATIONS_TOTAL, {inc, 1}}),

%    timer:sleep(?TASK_SLEEP),

    job(Connection, BranchIDs).

run(Connection) ->
    BranchIDs = get_branch_ids(Connection),
    metrics:create(meter, ?RATE),
    metrics:create(histogram, ?TIME),
    metrics:create(histogram, ?DOC_COUNT),
    metrics:create(counter, ?OPERATIONS_TOTAL),
    metrics:create(counter, ?OPERATIONS_ERR),
    metrics:create(counter, ?OPERATIONS_SUC),
    job(Connection, BranchIDs).
