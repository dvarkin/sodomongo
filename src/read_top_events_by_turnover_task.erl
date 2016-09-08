-module(read_top_events_by_turnover_task).
-author("eugeny").

-include("generator.hrl").
-define(QUERY_LIMIT, 10).

%% API

-export([init_metrics/0, job/2, init/1, start/3]).

init_metrics() ->
    gen_worker:init_metrics(?MODULE).

%%%===================================================================
%%% API
%%%===================================================================

start(ConnectionArgs, Time, SleepTimer) ->
    gen_worker:start(?MODULE, ConnectionArgs, Time, SleepTimer).

init(_Init_Args) ->
    #{
        query_limit => ?QUERY_LIMIT,
        branch_ids => []
    }.

job({MasterConn, _SlaveConn} = Conns, #{branch_ids := []} = State) ->
    BranchIds = get_branch_ids(MasterConn),
    job(Conns, State#{branch_ids := BranchIds});

job({MasterConn, _SlaveConn}, #{branch_ids := BranchIds, query_limit := Limit} = State) ->
    BranchId = util:rand_nth(BranchIds),
    {ok, query(MasterConn, BranchId, Limit), State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

query(Connection, BranchId, Limit) ->
    fun() ->
        Command = {
            <<"aggregate">>, <<"gameinfo">>,
            <<"pipeline">>, [
                {<<"$match">>, {<<"BranchID">>, BranchId}},
                {<<"$sort">>, {<<"GameBets.TotalDepositGBP">>, -1}},
                {<<"$limit">>, Limit}
            ]
        },

        Response = mc_worker_api:command(Connection, Command),
        util:parse_command_response(Response)
    end.

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