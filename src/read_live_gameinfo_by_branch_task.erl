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

-include("generator.hrl").

-define(TASK_SLEEP, 100).
-define(TASK, <<(atom_to_binary(?MODULE, utf8))/binary, "_metrics">>).
-define(RATE, <<?TASK/binary, ".rate">>).
-define(TIME, <<?TASK/binary, ".time">>).
-define(DOC_COUNT, <<?TASK/binary, ".documents_count">>).

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
    Query = #{
        ?BRANCH_ID => #{ <<"$eq">> => util:rand_nth(BranchIDs) },
        ?IS_ACTIVE => true
    },
    Collection = <<"gameinfo">>,
    Result = profiler:prof(
        ?TIME,
        fun() ->
            Cursor = mc_worker_api:find(
                Connection,
                Collection,
                Query
            ),
            mc_cursor:rest(Cursor)
        end),
    if
        Result == error -> error_logger:error_msg("Can't fetch response: ~p~n", [?MODULE]);
        true  ->  metrics:notify({?DOC_COUNT, length(Result)})
    end,

    metrics:notify({?RATE, 1}),

    timer:sleep(?TASK_SLEEP),

    job(Connection, BranchIDs).

run(Connection) ->
    BranchIDs = get_branch_ids(Connection),
    metrics:create(meter, ?RATE),
    metrics:create(histogram, ?TIME),
    metrics:create(histogram, ?DOC_COUNT),
    job(Connection, BranchIDs).