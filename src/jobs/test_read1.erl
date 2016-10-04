%%%-------------------------------------------------------------------
%%% @author Dmitry Omelechko <dvarkin@gmail.com>
%%% @copyright (C) 2016, Dmitry Omelechko
%%% @doc
%%%
%%% @end
%%% Created : 19 Aug 2016 by Dmitry Omelechko <dvarkin@gmail.com>
%%%-------------------------------------------------------------------
-module(test_read1).

-behaviour(gen_worker).

-export([init_metrics/0, job/2, init/1]).
-export([start/1]).

init_metrics() ->
    gen_worker:init_metrics(?MODULE).

%%%===================================================================
%%% API
%%%===================================================================


start(Args) -> 
    gen_worker:start_link(?MODULE, Args).

init( _Args) ->
    undefined.


job({_, Connection}, State) ->
    {ok, select(Connection), State}.


select(Connection) ->
    fun() -> 
            _R = mc_worker_api:find_one(Connection, <<"gameinfo">>, #{<<"ID">> => 1})
    end.

