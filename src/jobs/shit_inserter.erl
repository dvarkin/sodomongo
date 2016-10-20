%%%-------------------------------------------------------------------
%%% @author eugene
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 19. Oct 2016 3:34 PM
%%%-------------------------------------------------------------------
-module(shit_inserter).
-author("eugene").
-behavior(gen_worker).
-export([job/1, init/1, start/1, init_metrics/0]).


%% envs, aero, from, to

init(#{envs := Envs, from := From, to := To}) ->
    {ok, Aero} = aero:connect(Envs),
    io:format("shit_inserter[~p..~p]: started~n", [From, To]),
    #{aero => Aero, from => From, to => To}.


init_metrics() ->
    gen_worker:init_metrics(?MODULE).

start(Args) ->
    gen_worker:start_link(?MODULE, Args).

job(#{from := From, to := From} = State) ->
    {ok, fun() -> #{status => success, doc_count => 0, response => nothing_to_insert} end, State};
job(#{aero := Aero, from := From} = State) ->
    Data = shit_generator:gen(),
    {ok, fun() ->
        Response = aerospike:put(Aero, "ssd", "data", From, [{"data", Data}], 0),
        #{status => success, doc_count => 1, response => Response}
    end, State#{from := From + 1}}.


