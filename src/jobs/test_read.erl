%%%-------------------------------------------------------------------
%%% @author Dmitry Omelechko <dvarkin@gmail.com>
%%% @copyright (C) 2016, Dmitry Omelechko
%%% @doc
%%%
%%% @end
%%% Created : 19 Aug 2016 by Dmitry Omelechko <dvarkin@gmail.com>
%%%-------------------------------------------------------------------
-module(test_read).

-behaviour(gen_worker).

-export([init_metrics/0, job/1, init/1]).
-export([start/1]).

init_metrics() ->
    gen_worker:init_metrics(?MODULE).

%%%===================================================================
%%% API
%%%===================================================================


start(Args) -> 
    gen_worker:start_link(?MODULE, Args).

init(Args) ->
    RedisArgs = proplists:get_value(redis_connection, Args),
    RethinkArgs = proplists:get_value(rethink_connection, Args),
    error_logger:info_msg("~p~n~p~n", [RedisArgs, RethinkArgs]),
    undefined.


job(State) ->
    {ok, undefined, State}.


