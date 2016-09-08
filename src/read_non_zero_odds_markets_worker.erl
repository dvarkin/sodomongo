-module(read_non_zero_odds_markets_worker).
-author("eugeny").

-behavior(gen_worker).

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

init(_init_Args) ->
    undefined.

job({MasterConn, _}, State) ->
    {ok, query(MasterConn), State}.

query(Connection) ->
    fun() ->
        Query = #{<<"Selections.Odds">> => #{<<"$ne">> => 0}},
        Collection = <<"marketinfo">>,
        Cursor = mc_worker_api:find(
            Connection,
            Collection,
            Query
        ),
        util:parse_find_response(Cursor)
    end.
