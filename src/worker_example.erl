-module(worker_example).

-behaviour(gen_worker).

-include("generator.hrl").

%% API

-export([init_metrics/0, job/2, init/1, start/3]).

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
    error_logger:info_msg("iteration ~p", [State] ),
    {ok, some_query(Connection, SomeInfo), State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

some_query({MasterConn, _SecCon}, Info) ->
    fun() ->
        mc_worker_api:find_one(MasterConn, <<"odds">>, {<<"_id">>, Info}),
        #{doc_count => 1, status => success}
%            mc_worker_api:insert(Connection, ?GAMEINFO, GameInfo)
    end.
