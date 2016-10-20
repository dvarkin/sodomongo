%%%-------------------------------------------------------------------
%%% @author eugene
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 20. Oct 2016 4:41 PM
%%%-------------------------------------------------------------------
-module(primary_reader).
-author("eugene").
-behavior(gen_worker).
-export([job/1, init/1, start/1, init_metrics/0]).

-define(NAMESPACE, "ssd").
q-define(MAX_ID, 1000000).

init(#{envs := Envs, from := From, to := To}) ->
    {ok, Aero} = aero:connect(Envs),
    #{aero => Aero, from => From, to => To}.

init_metrics() ->
    gen_worker:init_metrics(?MODULE).

start(Args) ->
    gen_worker:start_link(?MODULE, Args).

job(#{aero := Aero} = State) ->
    {ok, fun() ->
        Response = aerospike:get(Aero, ?NAMESPACE, "data", rand:uniform(?MAX_ID), ["data"], 0),
        parse_response(Response)
         end, State}.

parse_response({citrusleaf_error, _} = Response) ->
    #{status => error, response => Response};
parse_response([{"data", _}|_] = Response) ->
    #{status => success, doc_count => 1, response => Response}.