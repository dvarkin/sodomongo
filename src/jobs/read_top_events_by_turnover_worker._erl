-module(read_top_events_by_turnover_worker).
-author("eugeny").

-behavior(gen_worker).

-include("../generator.hrl").
-define(QUERY_LIMIT, 10).

%% API

-export([init_metrics/0, job/2, init/1, start/1]).

init_metrics() ->
    gen_worker:init_metrics(?MODULE).

%%%===================================================================
%%% API
%%%===================================================================

start(Args) ->
    gen_worker:start_link(?MODULE, Args).

init(_) ->
    #{}.

job({_MasterConn, SlaveConn},State) ->
    BranchId = get_branch_id(),
    {ok, query(SlaveConn, BranchId, ?QUERY_LIMIT), State}.

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

        Response = mc_worker_api:command(Connection, Command, true),
        util:parse_command_response(Response)
    end.

get_branch_id() ->
    rand:uniform(30).

