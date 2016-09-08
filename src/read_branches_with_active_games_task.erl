-module(read_branches_with_active_games_task).
-author("eugeny").

-behaviour(gen_worker).

-include("generator.hrl").

-define(QUERY_LIMIT, 10).
-define(TASK_SLEEP, 1).
-define(HOUR_IN_SEC, 60 * 60).

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
    undefined.

job({_MasterConn, SlaveConn}, State) ->
    {ok, query(SlaveConn), State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

query(Connection) ->
    fun() ->
        Command = {
            <<"aggregate">>, <<"gameinfo">>,
            <<"pipeline">>, [
                #{
                    <<"$match">> => #{
                        <<"IsActive">> => true
                    }
                },
                #{
                    <<"$group">> => #{
                        <<"_id">> => <<"$BranchID">>,
                        <<"Events">> => #{
                            <<"$sum">> => 1
                        }
                    }
                },
                #{
                    <<"$project">> => #{
                        <<"_id">> => 0,
                        <<"BranchID">> => "$_id",
                        <<"Events">> => 1
                    }
                }
            ]
        },

        Response = mc_worker_api:command(Connection, Command),
        util:parse_command_response(Response)
    end.