-module(worker_example).

-behaviour(gen_worker).

-include("generator.hrl").

%% API

-export([start/3]).

%% gen_worker behaviour exports

-export([init_metrics/0, job/2, init/1]).

init_metrics() ->
    gen_worker:init_metrics(?MODULE).

%%%===================================================================
%%% API
%%%===================================================================

start(ConnectionArgs, Time, SleepTimer) -> 
    gen_worker:start(?MODULE, ConnectionArgs, Time, SleepTimer).

init(_Init_Args) ->
    {ok, undefined_state}.

job(Connection, State) ->
    SomeInfo = 1,
    {profile, some_query(Connection, SomeInfo), State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

some_query(Connection, Info) ->
    fun() ->
            mc_worker_api:find_one(Connection, <<"odds">>, {<<"_id">>, Info})
%            mc_worker_api:insert(Connection, ?GAMEINFO, GameInfo)
    end.
