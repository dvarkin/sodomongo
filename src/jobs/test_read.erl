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

-include("../generator.hrl").

-export([init_metrics/0, job/1, init/1]).
-export([start/0, start/1]).

init_metrics() ->
    gen_worker:init_metrics(?MODULE).

%%%===================================================================
%%% API
%%%===================================================================

start() ->
    start(#{module => ?MODULE, time => 60000, sleep => 10, envs => application:get_all_env(sodomongo)}).

start(Args) -> 
    gen_worker:start_link(?MODULE, Args).

init(Envs) ->
    {ok, Rethink} = rethinkdb:connect(Envs),
    #{rethink => Rethink}.

job(#{next := Next} = State) when is_map(Next) ->
    io:format("next: ~p~n", [Next]),
    Response = rethinkdb:next(Next),
    Parsed = parse_response(Response),
    io:format("Chenages response ~p ", [Response]),
    {ok, {send_metrics, Parsed}, State#{next := Response}};

job(#{rethink := Rethink} = State) ->
    Response = rethinkdb:r([
                            [table, ?GAMEINFO],
                            [changes]], 
                           Rethink),
    {ok, undefined, State#{next => Response}}.



parse_response(#{data := #{<<"inserted">> := Inserted}} = Response) when Inserted > 0 ->
    #{status => success, doc_count => Inserted, response => Response};
parse_response(#{data := #{<<"errors">> := Errors}} = Response) when Errors > 0 ->
    #{status => error, response => Response};
parse_response(Response) ->
    error_logger:error_msg("Unexpected response ~p", [Response]),
    #{status => error, reponse => Response}.
