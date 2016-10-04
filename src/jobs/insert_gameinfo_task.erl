%%%-------------------------------------------------------------------
%%% @author Dmitry Omelechko <dvarkin@gmail.com>
%%% @copyright (C) 2016, Dmitry Omelechko
%%% @doc
%%%
%%% @end
%%% Created : 19 Aug 2016 by Dmitry Omelechko <dvarkin@gmail.com>
%%%-------------------------------------------------------------------
-module(insert_gameinfo_task).

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

init(Envs) ->
    {ok, Conn} = rethinkdb:connect(Envs),
    #{conn => Conn}.


job(#{conn := Conn} = State) ->
    {ok, insert(Conn), State}.

insert(Conn) ->
    fun() ->
        rethinkdb:r([
            [table, <<"gameinfo">>],
            [insert, #{a => 1}]
        ], Conn)
    end.

