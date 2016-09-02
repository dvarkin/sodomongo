-module(read_non_zero_odds_markets_task).
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
    Query = #{<<"Selections.Odds">> => #{<<"$ne">> => 0}},
    Collection = <<"marketinfo">>,
    Result = profiler:prof(
        ?TIME,
        fun() ->
            Cursor = mc_worker_api:find(
                Connection,
                Collection,
                Query
            ),
            Data = mc_cursor:rest(Cursor),
            mc_cursor:close(Data),
            Data
        end),

    if
        Result == error ->
            begin
                error_logger:error_msg("Can't fetch response: ~p~n", [?MODULE]),
                metrics:notify({?OPERATIONS_ERR, {inc, 1}})
            end;
        true  ->
            begin
                metrics:notify({?DOC_COUNT, length(Result)}),
                metrics:notify({?OPERATIONS_SUC, {inc, 1}})
            end
    end,

    metrics:notify({?OPERATIONS_TOTAL, {inc, 1}}),
    metrics:notify({?RATE, 1}),
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