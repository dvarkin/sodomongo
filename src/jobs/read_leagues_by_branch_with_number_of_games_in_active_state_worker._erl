-module(read_leagues_by_branch_with_number_of_games_in_active_state_worker).
-author("eugeny").

-behavior(gen_worker).

-include("../generator.hrl").

-type state() :: #{redis_connection => pid()}.

%% API

-export([init_metrics/0, job/2, init/1, start/1]).

init_metrics() ->
    gen_worker:init_metrics(?MODULE).

%%%===================================================================
%%% API
%%%===================================================================

start(Args) ->
    gen_worker:start_link(?MODULE, Args).


-spec init(Args :: map()) -> state().
init(#{redis_conn_args := RedisConnArgs}) ->
    {ok, RedisConn} = apply(eredis, start_link, RedisConnArgs),
    #{
        redis_connection => RedisConn
    }.

job({_MasterConn, SlaveConn}, #{redis_connection := RedisConn} = State) ->
    BranchID = get_branch_id(RedisConn),
    {ok, query(SlaveConn, BranchID), State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

query(_, undefined) ->
    fun() ->
        #{status => error, error_reason => <<"No gameinfos in db">>}
    end;

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

get_branch_id(_RedisConnection) ->
    rand:uniform(30).
%%    case meta_storage:get_random_gameinfo(RedisConnection) of
%%        undefined -> undefined;
%%        #{?BRANCH_ID := BranchID} -> BranchID
%%    end.