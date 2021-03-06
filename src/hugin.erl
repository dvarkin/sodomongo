-module(hugin).
-behaviour(gen_server).

-define(SERVER, {global, ?MODULE}).

%% API.
-export([start_link/0]).

-export([worker_monitor/3, worker_state/3, worker_terminate/1]).

-export([workers/0, start_job/4]).

%% gen_server.
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).

-record(state, {workers = #{}, envs = []}).

-define(MAX_ID, 1000000).

%% API.

%% Worker API

worker_monitor(Worker, Task_Module, State) ->
    gen_server:cast(?SERVER, {worker_monitor, Worker, Task_Module, State }).

worker_state(Worker, Task_Module, State) ->
    gen_server:cast(?SERVER, {worker_state, Worker, Task_Module, State}).

worker_terminate(Worker) ->
    gen_server:cast(?SERVER, {worker_terminate, Worker}).

%% JOB API

workers() ->
    gen_server:call(?SERVER, workers).

start_job(Task_Module, Workers, Time, Sleep) when is_atom(Task_Module) andalso Workers > 0 andalso Time > 0 ->
    gen_server:call(?SERVER, {start_job, Task_Module, Workers, Time, Sleep}).

%% GEN server API

-spec start_link() -> {ok, pid()}.
start_link() ->
    gen_server:start_link(?SERVER, ?MODULE, [], []).

%% gen_server.

init([]) ->
    metrics:create(meter, <<"total_rate">>),
    metrics:create(gauge, <<"worker.active">>),
    metrics:create(gauge, <<"worker.awaiting">>),
    metrics:create(counter, <<"reconnections.total">>),
    metrics:create(counter, <<"timedout.total">>),
    Envs = application:get_all_env(sodomongo),
    io:format("~p~n", [Envs]),
    {ok, #state{envs = Envs}}.

handle_call(workers, _From, #state{workers = Workers} = State) ->
    {reply, Workers, State};

handle_call({start_job, Task_Module, WorkersNum, Time, Sleep}, _From, #state{envs = Envs} = State) ->
    M = ?MAX_ID div WorkersNum,
    [start_worker(N, #{from => M * N, to => M * N + M - 1, envs => Envs}, Task_Module, Time, Sleep) || N <- lists:seq(1, WorkersNum)],
    {reply, ok, State};

handle_call(_Request, _From, State) ->
    {reply, ignored, State}.

handle_cast({worker_monitor, Worker, Task_Module, WorkerState}, #state{workers = Workers} = State) ->
    {noreply, State#state{workers = Workers#{Worker => #{state => WorkerState, task => Task_Module}}}};

handle_cast({worker_state, Worker, Task_Module, WorkerState}, #state{workers = Workers} = State) ->
    {noreply, State#state{workers = Workers#{Worker := #{state => WorkerState, task => Task_Module}}}};

handle_cast({worker_terminate, Worker}, #state{workers = Workers} = State) ->
    W = maps:remove(Worker, Workers),
    {noreply, State#state{workers = W}}.

handle_info(_Info, State) ->
    error_logger:error_msg("Hugin. Unhendled message: ~p", [_Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    error_logger:error_msg("Hugin terminated ~p", [_Reason]),
    ok.

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%% Internal libs

gen_node() ->
    Nodes = nodes(),
    util:rand_nth(Nodes).

start_worker(N, Envs, Task_Module, Time, Sleep) when N > 0 ->
    spawn(fun() ->
		  timer:sleep(N * 20),
		  {ok, _Pid} = rpc:call(gen_node(), Task_Module, start, [#{envs => Envs,
                                                                           time => Time,
                                                                           sleep => Sleep
                                                                           }])
	  end).
