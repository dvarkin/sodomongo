%%%-------------------------------------------------------------------
%%% @author serhiinechyporhuk
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 26. Aug 2016 13:54
%%%-------------------------------------------------------------------
-module(read_live_gameinfo_by_branch_worker).
-author("serhiinechyporhuk").

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

init(_Init_Args) ->
   #{branches_ids => []}.

job({MasterConn, _} = Conns, #{branches_ids := []} = State) ->
    BranchesIDs = get_branch_ids(MasterConn),
    job(Conns, State#{branches_ids := BranchesIDs});

job({MasterConn, _}, #{branches_ids := BranchesIDs} = State) ->
    BranchID = util:rand_nth(BranchesIDs),
    {ok, query(MasterConn, BranchID), State}.

query(Connection, BranchID) ->
    fun() ->
        Query = #{
            ?BRANCH_ID => #{<<"$eq">> => BranchID},
            ?IS_ACTIVE => true
        },
        Collection = <<"gameinfo">>,
        Cursor = mc_worker_api:find(
            Connection,
            Collection,
            Query
        ),
        util:parse_find_response(Cursor)
    end.

get_branch_ids(Connection) ->
    Cursor = mc_worker_api:find(
        Connection,
        <<"gameinfo">>,
        #{},
        #{projector => {?BRANCH_ID, true}}
    ),

    Result = mc_cursor:rest(Cursor),
    BranchIDs = [BranchID || #{<<"BranchID">> := BranchID} <- Result],
    mc_cursor:close(Cursor),
    sets:to_list(sets:from_list(BranchIDs)).