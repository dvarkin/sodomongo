%%%-------------------------------------------------------------------
%%% @author eugeny
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 01. Sep 2016 16:31
%%%-------------------------------------------------------------------
-module(read_top_events_starting_soon_task).
-author("eugeny").

%% API
-export([run/1, query/1]).

-define(TASK, <<(atom_to_binary(?MODULE, utf8))/binary, "_metrics">>).
-define(RATE, <<?TASK/binary, ".rate">>).
-define(TIME, <<?TASK/binary, ".time">>).
-define(DOC_COUNT, <<?TASK/binary, ".documents_count">>).
-define(OPERATIONS, <<?TASK/binary, ".operations">>).
-define(OPERATIONS_TOTAL, <<?OPERATIONS/binary, ".total">>).
-define(OPERATIONS_ERR, <<?OPERATIONS/binary, ".err">>).
-define(OPERATIONS_SUC, <<?OPERATIONS/binary, ".suc">>).

-define(QUERY_LIMIT, 10).
-define(TASK_SLEEP, 1).
-define(HOUR_IN_SEC, 60 * 60).

mongo_datetime_with_offset(Timestamp, OffsetSet) ->
    SecInMega = 1000000,
    {Mega, Sec, _} = Timestamp,
    TotalSec = Mega * SecInMega + Sec,
    NewSec = TotalSec + OffsetSet,
    {NewSec div SecInMega, NewSec rem SecInMega, 0}.

query(Connection) ->
    MaxStartDate = mongo_datetime_with_offset(os:timestamp(), ?HOUR_IN_SEC),

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
                <<"$limit">>, ?QUERY_LIMIT
            }
        ]
    },
    {_, Data} = Response = profiler:prof(?TIME, fun() -> mc_worker_api:command(Connection, Command) end),

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

    Data.

job(Connection) ->
    query(Connection),
    job(Connection).

run(Connection) ->
    metrics:create(meter, ?RATE),
    metrics:create(histogram, ?TIME),
    metrics:create(histogram, ?DOC_COUNT),
    job(Connection).