-module(kinder).
-behaviour(gen_fsm).

-define(CONNECTION_TIMEOUT, 10000).

%% API.
-export([job/3]).
-export([stop_job/1]).
-export([state/1]).
-export([start/1, start/3]).
-export([stop/1]).
-export([connect_to_mongo/1]).

%% STATES

-export(['WAIT_CONNECTION'/2, 'WAIT_CONNECTION'/3]).
-export(['WAIT_JOB'/2, 'WAIT_JOB'/3]).
-export(['INPROGRESS'/2, 'INPROGRESS'/3]).


%% gen_fsm.
-export([init/1]).
-export([handle_event/3]).
-export([handle_sync_event/4]).
-export([handle_info/3]).
-export([terminate/3]).
-export([code_change/4]).

-define(OPERATION_TIMED_OUT, {{bad_query, #{<<"code">> := 50}}, _}).

-record(state, {
    connection :: pid() | undefined,
    connection_args :: list(),
    task_module :: atom(),
    task_time :: pos_integer(),
    task_pid :: pid() | undefined
}).

%% API.

-spec job(Pid :: pid(), TaskModule :: atom(), Time :: pos_integer()) -> ok.

job(Pid, TaskModule, Time) ->
    gen_fsm:send_event(Pid, {job, TaskModule, Time}).

-spec stop(Pid :: pid()) -> ok.

stop_job(Pid) ->
    gen_fsm:send_event(Pid, stop_job).

-spec state(Pid :: pid()) -> #state{}.

state(Pid) ->
    gen_fsm:sync_send_all_state_event(Pid, state).

-spec start(Connection :: list()) -> {ok, pid()} | ingonre | {ok, pid()}.

start(ConnectionArgs) ->
    start(ConnectionArgs, undefined, 1).

-spec start(Connection :: list(), atom(), pos_integer()) -> {ok, pid()} | ingonre | {ok, pid()}.

start([_|_] = ConnectionArgs, Task_Module, Time) when is_atom(Task_Module) andalso Time > 0 ->
    gen_fsm:start(?MODULE, [ConnectionArgs, Task_Module, Time], []).

-spec stop_job(Pid :: pid()) -> ok.

stop(Pid) ->
    gen_fsm:stop(Pid).

%% gen_fsm.

init([ConnectionArgs, Task_Module, Time]) ->
    process_flag(trap_exit, true),
    self() ! connect,
    hugin:worker_monitor(self(), Task_Module, 'WAIT_CONNECTION'),
    {ok, 'WAIT_CONNECTION', #state{connection_args = ConnectionArgs, task_module = Task_Module, task_time = Time}}.

%% ASYNC STATES
'WAIT_JOB'({job, Task_Module, RTime}, #state{connection = Connection} = StateData) ->
    hugin:worker_state(self(), Task_Module, 'INPROGRESS'),
    P = start_job(Connection, Task_Module, RTime),
    {next_state, 'INPROGRESS', StateData#state{task_module = Task_Module, task_time = RTime, task_pid = P}};

'WAIT_JOB'(_Event, StateData) ->
    {next_state, 'WAIT_JOB', StateData}.

'WAIT_CONNECTION'(_Event, StateData) ->
    {next_state, 'WAIT_CONNECTION', StateData}.

'INPROGRESS'(stop_job, #state{task_pid = Pid, task_module = Task_Module} = StateData) ->
    erlang:exit(Pid, kill),
    hugin:worker_state(self(), Task_Module, 'WAIT_JOB'),
    {next_state, 'WAIT_JOB', StateData};

'INPROGRESS'(_Event, StateData) ->
    {next_state, 'INPROGRESS', StateData}.

%% SYNC STATES

'WAIT_JOB'(_Event, _From, StateData) ->
    {reply, ignored, 'WAIT_JOB', StateData}.

'INPROGRESS'(_Event, _From, StateData) ->
    {reply, ignored, 'INPROGRESS', StateData}.

'WAIT_CONNECTION'(_Event, _From, StateData) ->
    {reply, ignored, 'WAIT_CONNECTION', StateData}.

%%%%%%%%%%%%%% Internal functions %%%%%%%%%%%%%%%%


handle_event(_Event, StateName, StateData) ->
    {next_state, StateName, StateData}.

handle_sync_event(state, _From, StateName, StateData) ->
    Reply = {StateName, StateData},
    {reply, Reply, StateName, StateData};

handle_sync_event(_Event, _From, StateName, StateData) ->
    {reply, ignored, StateName, StateData}.

handle_info(connect, 'WAIT_CONNECTION', #state{connection_args = ConnectionArgs, task_module = Task_Module, task_time = Time} = StateData) 
  when Task_Module /= undefined ->
    
    %% CONNECT TO MONGO 
    %% Start task immediately

    {ok, Connection} = connect_to_mongo(ConnectionArgs),
    
    P = start_job(Connection, Task_Module, Time),

    hugin:worker_monitor(self(), Task_Module, 'INPROGRESS'),
    
    {next_state, 'INPROGRESS', StateData#state{connection  = Connection, task_pid = P}};

handle_info(connect, 'WAIT_CONNECTION', #state{connection_args = ConnectionArgs, task_module = undefined} = StateData) ->
  
    %% JUST CONNECT TO MONGO

    {ok, Connection} = connect_to_mongo(ConnectionArgs),

    hugin:worker_monitor(self(), undefined, 'WAIT_JOB'),
    {next_state, 'WAIT_JOB', StateData#state{connection  = Connection}};

handle_info({'EXIT', Connection, Reason}, _StateName, #state{connection = Connection} = State) ->
    error_logger:error_msg("Connection lost: ~p",[Reason]),
    metrics:notify({<<"reconnections.total">>, {inc, 1}}),
    hugin:worker_monitor(self(), undefined,'WAIT_CONNECTION'),
    self() ! connect,
    {next_state, 'WAIT_CONNECTION', State#state{connection = undefined, task_pid = undefined}};

%% tcp abnormal termination
handle_info({tcp_closed, _Pid}, _StateName, #state{connection = Connection} = State) ->
    erlang:exit(Connection),
    {next_state, 'WAIT_CONNECTION', State#state{connection = undefined, task_pid = undefined, task_module = undefined, task_time = 1}};

%% normal exit for task
handle_info({'EXIT', Pid, killed}, _StateName, #state{task_pid = Pid} = State) ->
    hugin:worker_monitor(self(), undefined,'WAIT_JOB'),
    {next_state, 'WAIT_JOB', State#state{task_pid = undefined, task_module = undefined, task_time = 1}};

%% termination of task process
handle_info({'EXIT', OldPid, ?OPERATION_TIMED_OUT}, _StateName, #state{task_time = RTime, task_module = Task_Module, connection = Connection, task_pid = OldPid} = State) ->
    error_logger:error_msg("Operation timed out, module: ~p", [Task_Module]),
    metrics:notify({<<"timedout.total">>, {inc, 1}}),
    metrics:notify({<<(atom_to_binary(Task_Module, utf8))/binary, "_metrics.operations.err">>, {inc, 1}}),
    metrics:notify({<<(atom_to_binary(Task_Module, utf8))/binary, "_metrics.operations.total">>, {inc, 1}}),
    P = start_job(Connection, Task_Module, RTime),
    {next_state, 'INPROGRESS', State#state{task_pid = P}};

handle_info({'EXIT', Pid, Error}, _StateName, #state{task_pid = Pid} = State) ->
    error_logger:error_msg("Task terminate with: ~p", [Error]),
    hugin:worker_monitor(self(), undefined,'WAIT_JOB'),
    {next_state, 'WAIT_JOB', State#state{task_pid = undefined, task_module = undefined, task_time = 1}};


handle_info(_Info, StateName, StateData) ->
    error_logger:error_msg("Unhandeled info msg: ~p ~p ~p",[_Info, StateName, StateData]),
    {next_state, StateName, StateData}.



terminate(Reason, _StateName, _StateData) ->
    error_logger:error_msg("KINDER TERMINATE  msg: ~p",[Reason]),
    hugin:worker_terminate(self()),
    ok.

code_change(_OldVsn, StateName, StateData, _Extra) ->
	{ok, StateName, StateData}.


%%%% INTERNALFUNCTIONS $$$$$

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

-spec start_job(Connection :: pid(), Task_Module :: atom(), Time :: pos_integer()) -> pid() 
											  | {error, Error :: any()} 
											  | {ok, timer:tref()}. 

start_job(Connection, Task_Module, Time) when is_pid(Connection) andalso is_atom(module) andalso Time > 0 ->
    P = spawn_link(Task_Module, run, [Connection]),
    timer:kill_after(timer:seconds(Time), P),
    P.
