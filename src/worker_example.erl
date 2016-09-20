-module(worker_example).

-behaviour(gen_worker).

-include("generator.hrl").

%% API
-export([init_metrics/0, job/2, init/1, start/3]).

%%%===================================================================
%%% API
%%%===================================================================

init_metrics() ->
    gen_worker:init_metrics(?MODULE).

start(ConnectionArgs, Time, SleepTimer) ->
    gen_worker:start(?MODULE, ConnectionArgs, Time, SleepTimer).

init(_Init_Args) ->
    #{branches_ids => []}.


job({_, SlaveConn}, State) ->
    {ok, query(SlaveConn), State}.

query(Connection) ->
    fun() ->
        Query = #{
            ?ID => 1
        },
        Collection = <<"gameinfo">>,
        Cursor = mc_worker_api:find(
            Connection,
            Collection,
            Query
        ),
        util:parse_find_response(Cursor)
    end.
