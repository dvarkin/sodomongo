%%%-------------------------------------------------------------------
%%% @author eugeny
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 31. Aug 2016 18:33
%%%-------------------------------------------------------------------
-module(query_branches_with_active_games).
-author("eugeny").

%% API
-export([run/1]).

-define(TASK_SLEEP, 1).
-define(TASK, <<(atom_to_binary(?MODULE, utf8))/binary, "_metrics">>).
-define(RATE, <<?TASK/binary, ".rate">>).
-define(TIME, <<?TASK/binary, ".time">>).
-define(DOC_COUNT, <<?TASK/binary, ".documents_count">>).

job(Connection) ->
    Command = #{
        <<"aggregate">> => <<"gameinfo">>,
        <<"pipeline">> => [
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
    {Status, #{<<"result">> := Result}} = profiler:prof(
        ?TIME,
        fun() ->
            mc_worker_api:command(Connection, Command)
        end),

    if
        Status /= true -> error_logger:error_msg("Can't fetch response: ~p~n", [?MODULE]);
        true  ->  metrics:notify({?DOC_COUNT, length(Result)})
    end,

    metrics:notify({?RATE, 1}),
    timer:sleep(?TASK_SLEEP),
    job(Connection).

run(Connection) ->
    metrics:create(meter, ?RATE),
    metrics:create(histogram, ?TIME),
    metrics:create(histogram, ?DOC_COUNT),
    job(Connection).