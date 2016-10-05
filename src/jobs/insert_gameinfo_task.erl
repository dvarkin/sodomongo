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

-include("../generator.hrl").

-define(LIMIT, 1000).


%% API
-export([start/1, start/0]).

%% gen_worker behaviour API
-export([init_metrics/0, job/1, init/1, insert/2]).

init_metrics() ->
    gen_worker:init_metrics(?MODULE).

%%%===================================================================
%%% API
%%%===================================================================

start() ->
    ConnArgs = mongo:conn_args(),
    {ok, RedisArgs} = application:get_env(sodomongo, redis_connection),
    start(#{mongo_conn_args => ConnArgs, time => 5000, sleep => 1000, redis_conn_args => RedisArgs}).


-spec start(Args :: map()) -> {ok, pid()}.

start(Args) ->
    gen_worker:start_link(?MODULE, Args).

-spec init(map()) -> map().

init(Envs) ->
    {ok, Rethink} = rethinkdb:connect(Envs),
    {ok, Redis} = redis:connect(Envs),
    #{rethink => Rethink, redis => Redis}.

-spec job(State :: term()) -> {ok, fun(), term()} | {ok, undefined, term()}.

job(#{redis := Redis, rethink := Rethink} = State) ->
    case meta_storage:games_size(Redis) > ?LIMIT of
        true ->
            {ok, undefined, State};
        false ->
            {GameInfo, Markets} = generator:new_game_with_markets(),
            meta_storage:insert_game(Redis, GameInfo, Markets),
            {ok, insert(Rethink, GameInfo), State}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

insert(Conn, GameInfo) ->
    fun() ->
        Response = rethinkdb:r([
            [table, ?GAMEINFO],
            [insert, GameInfo]
        ], Conn),
        parse_response(Response)
    end.

parse_response(#{data := #{<<"inserted">> := Inserted}} = Response) when Inserted > 0 ->
    #{status => success, doc_count => Inserted, response => Response};
parse_response(#{data := #{<<"errors">> := Errors}} = Response) when Errors > 0 ->
    #{status => error, response => Response}.

