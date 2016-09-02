%%%-------------------------------------------------------------------
%%% @author eugeny
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 31. Aug 2016 18:33
%%%-------------------------------------------------------------------
-module(read_branches_with_active_games_task).
-author("eugeny").

%% API
-export([run/1]).

-define(TASK_SLEEP, 1).
-define(TASK, <<(atom_to_binary(?MODULE, utf8))/binary, "_metrics">>).
-define(RATE, <<?TASK/binary, ".rate">>).
-define(TIME, <<?TASK/binary, ".time">>).
-define(DOC_COUNT, <<?TASK/binary, ".documents_count">>).
-define(OPERATIONS, <<?TASK/binary, ".operations">>).
-define(OPERATIONS_TOTAL, <<?OPERATIONS/binary, ".total">>).
-define(OPERATIONS_ERR, <<?OPERATIONS/binary, ".err">>).
-define(OPERATIONS_SUC, <<?OPERATIONS/binary, ".suc">>).

job(Connection) ->
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
    Response = profiler:prof(?TIME, fun() -> mc_worker_api:command(Connection, Command) end),

    case Response of
        {false, _} ->
            begin
                error_logger:error_msg("Can't read in module: ~p~n, response: ~p~n", [?MODULE, Response]),
                metrics:notify({?OPERATIONS_ERR, {inc, 1}})
            end;
        {true,  #{ <<"result">> := Result }} ->
            begin
                metrics:notify({?DOC_COUNT, length(Result)}),
                metrics:notify({?OPERATIONS_SUC, {inc, 1}})
            end
    end,

    metrics:notify({?RATE, 1}),
    metrics:notify({?OPERATIONS_TOTAL, {inc, 1}}),
    timer:sleep(?TASK_SLEEP),
    job(Connection).

run(Connection) ->
    metrics:create(meter, ?RATE),
    metrics:create(histogram, ?TIME),
    metrics:create(histogram, ?DOC_COUNT),
    metrics:create(counter, ?OPERATIONS_TOTAL),
    metrics:create(counter, ?OPERATIONS_ERR),
    metrics:create(counter, ?OPERATIONS_SUC),
    job(Connection).