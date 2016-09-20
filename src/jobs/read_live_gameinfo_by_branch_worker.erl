%%%-------------------------------------------------------------------
%%% @author serhiinechyporhuk
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 26. Aug 2016 13:54
%%%-------------------------------------------------------------------
-module(read_live_gameinfo_by_branch_worker).
-author("serhiinechyporhuk").

-behavior(gen_worker).

-include("../generator.hrl").

%% API
-export([init_metrics/0, job/2, init/1, start/1]).

%%%===================================================================
%%% API
%%%===================================================================

init_metrics() ->
    gen_worker:init_metrics(?MODULE).

start(Args) ->
    gen_worker:start_link(?MODULE, Args).

init(#{redis_conn_args := RedisConnArgs}) ->
    {ok, RedisConn} = apply(eredis, start_link, RedisConnArgs),
    #{redis_connection => RedisConn}.


job({_, SlaveConn}, #{redis_connection := RedisConn} = State) ->
    BranchID = get_branch_id(RedisConn),
    {ok, query(SlaveConn, BranchID), State}.

query(Connection, BranchID) ->
    fun() ->
        Query = #{
            ?BRANCH_ID => #{<<"$eq">> => BranchID},
            ?IS_ACTIVE => true
        },
        Collection = <<"gameinfo">>,
        Cursor = mc_worker_api:find(
            Connection,
            Collection,
            Query
        ),
        util:parse_find_response(Cursor)
    end.

get_branch_id(RedisConnection) ->
    case meta_storage:get_random_gameinfo(RedisConnection) of
        undefined -> undefined;
        #{?BRANCH_ID := BranchID} -> BranchID
    end.