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
-export([init_metrics/0, job/2, init/1, start/1, start/0]).

%%%===================================================================
%%% API
%%%===================================================================

init_metrics() ->
    gen_worker:init_metrics(?MODULE).

start() ->
    ConnArgs = mongo:conn_args(),
    error_logger:info_msg("~p~n", [ConnArgs]),
    {ok, RedisArgs} = application:get_env(sodomongo, redis_connection),
    start(#{mongo_conn_args => ConnArgs, time => 20000, sleep => 1, redis_conn_args => RedisArgs}).


start(Args) ->
    gen_worker:start_link(?MODULE, Args).

init(#{redis_conn_args := _RedisConnArgs}) ->
%%    {ok, RedisConn} = apply(eredis, start_link, RedisConnArgs),
%%    #{redis_connection => RedisConn},
    ok.


job({_, SlaveConn}, State) ->
%%    BranchID = get_branch_id(RedisConn),
    {ok, query(SlaveConn, rand:uniform(30)), State}.

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

%%get_branch_id(RedisConnection) ->
%%    case meta_storage:get_random_gameinfo(RedisConnection) of
%%        undefined -> undefined;
%%        #{?BRANCH_ID := BranchID} -> BranchID
%%    end.
