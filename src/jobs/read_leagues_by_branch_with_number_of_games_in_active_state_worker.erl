-module(read_leagues_by_branch_with_number_of_games_in_active_state_worker).
-author("eugeny").

-behavior(gen_worker).

-include("generator.hrl").
-define(QUERY_LIMIT, 10).

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
    #{
        query_limit => ?QUERY_LIMIT,
        branch_ids => []
    }.

job({_MasterConn, SlaveConn} = Conns, #{branch_ids := []} = State) ->
    BranchIds = get_branch_ids(SlaveConn),
    job(Conns, State#{branch_ids := BranchIds});

job({_MasterConn, SlaveConn}, #{branch_ids := BranchIds} = State) ->
    BranchId = util:rand_nth(BranchIds),
    {ok, query(SlaveConn, BranchId), State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

query(Connection, BranchId) ->
    fun() ->
        IsLive = util:rand_nth([true, false]),
        Command = {
            <<"aggregate">>, <<"gameinfo">>,
            <<"pipeline">>, [
                {
                    <<"$match">>,
                    {
                        <<"BranchID">>, BranchId,
                        <<"IsLive">>, IsLive,
                        <<"IsActive">>, true
                    }
                },
                {
                    <<"$group">>,
                    {
                        <<"_id">>, <<"$LeagueID">>,
                        <<"ActiveEvents">>, {<<"$sum">>, 1}
                    }
                }
            ]
        },

        Response = mc_worker_api:command(Connection, Command, true),
        util:parse_command_response(Response)
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