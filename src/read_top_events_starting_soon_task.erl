-module(read_top_events_starting_soon_task).
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
    #{
        query_limit => ?QUERY_LIMIT,
        max_start_date => util:timestamp_with_offset(os:timestamp(), ?HOUR_IN_SEC)
    }.

job({MasterConn, _SecConn}, State) ->
    {ok, query(MasterConn, maps:with([query_limit, max_start_date], State)), State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

query(Connection, #{query_limit := Limit, max_start_date := MaxStartDate}) ->
    fun() ->
        Command = {
            <<"aggregate">>, <<"gameinfo">>,
            <<"pipeline">>, [
                {
                    <<"$match">>, {
                    <<"StartDate">>, {
                        <<"$lt">>, MaxStartDate
                    }
                }
                },
                {
                    <<"$sort">>, {
                    <<"StartDate">>, 1
                }
                },
                {
                    <<"$limit">>, Limit
                }
            ]
        },

        Response = mc_worker_api:command(Connection, Command),
        util:parse_command_response(Response)
    end.
