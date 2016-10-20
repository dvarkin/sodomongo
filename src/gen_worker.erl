%%%-------------------------------------------------------------------
%%% @author Dmitry Omelechko <dvarkin@gmail.com>
%%% @copyright (C) 2016, Dmitry Omelechko
%%% @doc
%%%
%%% @end
%%% Created :  5 Sep 2016 by Dmitry Omelechko <dvarkin@gmail.com>
%%%-------------------------------------------------------------------
-module(gen_worker).

-behaviour(gen_server).

-export([start_link/2, init_metrics/1]).

-type metrics() :: #{ doc_count => pos_integer(),
                      error_reason => any(),
                      result => any(),
                      status => success | error }.
-type action_closure() :: fun( () -> metrics() ).

-callback init_metrics() -> ok.
-callback init(Args :: list()) -> term().
-callback job(State :: term()) ->
    {ok, ActionClosure :: action_closure(), State :: term()}.
-callback start(Args :: map()) -> {ok, pid()}.

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).


-define(SERVER, ?MODULE).

-record(state, {envs :: [],
                module :: atom(),
                time :: pos_integer(), 
                sleep :: integer(),
                metrics :: map(),
                module_state :: term()
               }).

start_link(Module, Args) ->
    gen_server:start_link(?MODULE, [Args#{module => Module}], []).

init([#{ module := Module, time := Time, sleep := Sleep, envs := Envs }]) ->
    timer:apply_after(Time, gen_server, stop, [self()]),
    Metrics = make_metrics_titles(Module),
    Module_State = Module:init(Envs),
    self() ! tick,
    hugin:worker_monitor(self(), Module, 'INPROGRESS'),
    {ok, #state{envs = Envs,
                module = Module, 
                module_state = Module_State,
                time = Time, 
                sleep = Sleep,
                metrics = Metrics}}.

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.


handle_info(tick, #state{module = Module, sleep = Sleep, module_state = Module_State, metrics = Metrics} = State) ->
    Response = Module:job(Module_State),
    Module_State_New = parse_response(Response, Module, Metrics),
    idle(Sleep),
    {noreply, State#state{module_state = Module_State_New}};

handle_info(_Info, State) ->
    io:format("Unhandeled message in worker: ~p", [_Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    hugin:worker_terminate(self()),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%% Init Metrics

make_metrics_titles(Module) when is_atom(Module) ->
    M = <<(atom_to_binary(Module, utf8))/binary, "_metrics">>,
    Rate = <<M/binary, ".rate">>,
    Time = <<M/binary, ".time">>,
    DocsCount = <<M/binary, ".documents_count">>,
    O = <<M/binary, ".operation">>,
    Total = <<O/binary, ".total">>,
    Err = <<O/binary, ".err">>,
    Suc = <<O/binary, ".suc">>,
    #{rate => {meter, Rate}, 
      time => {histogram, Time},
      docs_count => {histogram, DocsCount},
      total => {counter, Total},
      success => {counter,  Suc},
      error => {counter,  Err}}.

init_metrics(Module) ->
    Metrics = maps:values(make_metrics_titles(Module)),
    start_metrics(Metrics).

start_metrics([{MetricType, Metric} | Metrics] ) ->
    metrics:create(MetricType, Metric),
    start_metrics(Metrics);
start_metrics([]) ->
    ok.

parse_response({ok, {send_metrics, Response}, Module_State_New}, Module, Metrics) ->
    send_metrics(Module, Metrics, Response),
    Module_State_New;
parse_response({ok, undefined, Module_State_New}, _Module, _Metrics) ->
    Module_State_New;
parse_response({ok, ProfileAction, Module_State_New}, Module, Metrics) ->
    profile_job(Module, ProfileAction, Metrics),
    Module_State_New.



idle(Sleep) when Sleep > 0 ->
    timer:send_after(Sleep, tick);
idle(_) ->
    self() ! tick.

%%% JOB

profile_job(Module, Action, #{time := {_, Time}} = Metrics) ->
    Response = profiler:prof(Time, Action),
    send_metrics(Module, Metrics, Response).

send_metrics(_Module, #{
               rate := {_, Rate},
               total := {_, Total},
               time := {_, _Time},
               error := {_, _Error},
               docs_count := {_, DocsCount},
               success := {_, Success}
              },
             #{status := success, doc_count := N} = Response) ->
    metrics:notify({Rate, 1}),
    metrics:notify({Total, {inc, 1}}),
    metrics:notify({DocsCount, N}),
    metrics:notify({Success, {inc, 1}});

send_metrics(Module, #{
               rate := {_, Rate},
               total := {_, Total},
               time := {_, _Time},
               error := {_, Error},
               docs_count := {_, _DocsCount},
               success := {_, _Success}
              },
             Response) ->
    metrics:notify({Rate, 1}),
    metrics:notify({Total, {inc, 1}}),
    metrics:notify({Error, {inc, 1}}),
    error_logger:error_msg("Error from module: ~p~n: ~p~n", [Module, Response]).
    
    
