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

-export([start_link/3, start_link/4, init_metrics/1, start/4]).

-type metrics() :: #{ doc_count => pos_integer(),
                      error_reason => any(),
                      status => success | error }.
-type action_closure() :: fun( () -> metrics() ).

-callback init_metrics() -> ok.
-callback init(Args :: list()) -> term().
-callback job(Connection :: pid(), State :: term()) ->
    {ok, ActionClosure :: action_closure(), State :: term()}.
-callback start(Args :: list(), Time :: pos_integer(), Sleep :: pos_integer() | undefined) ->
    {ok, pid()}.

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).




-define(SERVER, ?MODULE).

-record(state, {connection_args :: list(), 
                connection :: pid(),
                module :: atom(), 
                time :: pos_integer(), 
                sleep :: integer(),
                metrics :: map(),
                module_state :: term()
               }).

start(Module, ConnectionArgs, Time, Sleep) ->
    start_link(Module, ConnectionArgs, Time, Sleep).

start_link(Module, Args, Time) ->
    start_link(Module, Args, Time, undefined).
start_link(Module, Args, Time, Sleep) ->
    gen_server:start_link(?MODULE, [Module, Args, Time, Sleep], []).

init([Module, ConnectionArgs, Time, Sleep] = Args) ->
    self() ! connect,
    timer:exit_after(Time, normal),
    Metrics = make_metrics_titles(Module),
    hugin:worker_monitor(self(), Module, 'WAIT_CONNECTION'),
    Module_State = Module:init(Args),
    {ok, #state{connection_args = ConnectionArgs, 
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

handle_info(connect, #state{connection_args = ConnectionArgs, module = Module} = State) ->
    {ok, Connection} = connect_to_mongo(ConnectionArgs),
    self() ! tick, 
    hugin:worker_monitor(self(), Module, 'INPROGRESS'),
    {noreply, State#state{connection = Connection}};

handle_info(tick, #state{module = Module, connection = Connection, sleep = Sleep, module_state = Module_State, metrics = Metrics} = State) ->
    Response = Module:job(Connection, Module_State),
    Module_State_New = parse_response(Response, Module, Metrics),
    idle(Sleep),
    {noreply, State#state{module_state = Module_State_New}};

handle_info(_Info, State) ->
    error_logger:error_msg("Unhandeled message in worker: ~p", [_Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private %%
select_host(Hosts) ->
    Index = rand:uniform(length(Hosts)),
    lists:nth(Index, Hosts).

connect_to_mongo(ConnectionArgs) ->
    Hosts = proplists:get_value(host, ConnectionArgs),
    Host = select_host(Hosts),
    WithOneHost = lists:keyreplace(host, 1, ConnectionArgs, {host, Host}),
    mc_worker_api:connect(WithOneHost).

%%% INTERNAL

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

parse_response({ok, ProfileAction, Module_State_New}, Module, Metrics) ->
    profile_job(Module, ProfileAction, Metrics),
    Module_State_New.


idle(Sleep) when Sleep > 0 ->
    timer:send_after(Sleep, tick);
idle(_) ->
    self() ! tick.
    


%%% JOB 



profile_job(Module, Action,
                #{
                  rate := {_, Rate},
                  total := {_, Total},
                  time := {_, Time},
                  error := {_, Error},
                  docs_count := {_, DocsCount},
                  success := {_, Success}
                 }
               ) ->
    metrics:notify({Rate, 1}),
    metrics:notify({Total, {inc, 1}}),
    Response = profiler:prof(Time, Action),
    case Response of
        #{status := success, doc_count := N} ->
            begin
                metrics:notify({DocsCount, N}),
                metrics:notify({Success, {inc, 1}})
            end;
        #{status := error, error_reason := Reason} ->
            begin
                error_logger:error_msg("Error from module: ~p~n: ~p~n", [Module, Reason]),
                metrics:notify({Error, {inc, 1}})
            end
    end.
%%    case Response of
%%        {false, _} ->
%%            begin
%%                error_logger:error_msg("Can't make query from module: ~p~n, response: ~p~n", [Module, Response]),
%%                metrics:notify({Error, {inc, 1}})
%%            end;
%%        {true, #{ <<"writeErrors">> := WriteErrors}} ->
%%            begin
%%                error_logger:error_msg("Can't make query from module: ~p~n, error: ~p~n", [Module, WriteErrors]),
%%                metrics:notify({Error, {inc, 1}})
%%            end;
%%        {true,  #{ <<"n">> := N }} ->
%%            begin
%%                metrics:notify({DocsCount, N}),
%%                metrics:notify({Success, {inc, 1}})
%%            end
%%    end.