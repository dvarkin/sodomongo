-module(read_non_zero_odds_markets_worker).
-author("eugeny").

-behavior(gen_worker).

-include("../generator.hrl").

%% API
-export([init_metrics/0, job/2, init/1, start/1]).

%%%===================================================================
%%% API
%%%===================================================================

init_metrics() ->
    gen_worker:init_metrics(?MODULE).

start(Args) ->
    gen_worker:start_link(?MODULE, Args).

init(_init_Args) ->
    undefined.

job({_, SlaveConn}, State) ->
    {ok, query(SlaveConn), State}.

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
