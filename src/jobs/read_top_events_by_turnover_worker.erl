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

init(#{redis_connection_args := RedisConnArgs}) ->
    {ok, RedisConn} = apply(eredis, start_link, RedisConnArgs),
    #{redis_connection => RedisConn}.

job({_MasterConn, SlaveConn}, #{redis_connection := RedisConn} = State) ->
    BranchId = get_branch_id(RedisConn),
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

get_branch_id(Connection) ->
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
