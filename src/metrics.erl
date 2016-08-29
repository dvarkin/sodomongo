-module(metrics).
-author("serhiinechyporhuk").

-behaviour(gen_server).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-export([notify/1,
    create/2]).

-define(SERVER, {global, ?MODULE}).

-record(state, {}).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link(?SERVER, ?MODULE, [], []).

create(Type, Name) ->
    case folsom_metrics:metric_exists(Name) of
        true -> folsom_metrics:delete_metric(Name);
        _ -> ok
    end,
    gen_server:cast(?SERVER, {create, Type, Name}).

notify(Arg) ->
    gen_server:cast(?SERVER, {notify, Arg}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    {ok, #state{}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({notify, Arg}, State) ->
    folsom_metrics:notify(Arg),
    {noreply, State};

handle_cast({create, meter, Name}, State) ->
    folsom_metrics:new_meter(Name),
    {noreply, State};

handle_cast({create, histogram, Name}, State) ->
    folsom_metrics:new_histogram(Name),
    {noreply, State};

handle_cast({create, gauge, Name}, State) ->
    folsom_metrics:new_gauge(Name),
    {noreply, State};

handle_cast(Request, State) ->
    error_logger:error_msg("unhandled_cast: request = ~p", [Request]),
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.