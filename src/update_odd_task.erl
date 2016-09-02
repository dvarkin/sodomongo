-module(update_odd_task).
-include("generator.hrl").

-export([run/1]).

-define(TASK_SLEEP, 10).
-define(TASK, <<(atom_to_binary(?MODULE, utf8))/binary, "_metrics">>).
-define(RATE, <<?TASK/binary, ".rate">>).
-define(TIME, <<?TASK/binary, ".time">>).
-define(DOC_COUNT, <<?TASK/binary, ".documents_count">>).
-define(OPERATIONS, <<?TASK/binary, ".operations">>).
-define(OPERATIONS_TOTAL, <<?OPERATIONS/binary, ".total">>).
-define(OPERATIONS_ERR, <<?OPERATIONS/binary, ".err">>).
-define(OPERATIONS_SUC, <<?OPERATIONS/binary, ".suc">>).
-define(DEV_GEN_TIME, <<?TASK/binary, ".dev.gen_time">>).

%%%===================================================================
%%% API
%%%===================================================================

%% create_indexes(Connection) ->
%%     mc_worker_api:ensure_index(Connection, ?MARKETINFO, #{<<"key">> => {?ID, <<"hashed">>}}),
%%     mc_worker_api:ensure_index(Connection, ?MARKETINFO, #{<<"key">> => {<<"Selections.ID">>,1}}).



run(Connection) ->
    
    %% init metrics
    metrics:create(meter, ?RATE),
    metrics:create(histogram, ?DEV_GEN_TIME),
    metrics:create(histogram, ?TIME),
    metrics:create(histogram, ?DOC_COUNT),
    metrics:create(counter, ?OPERATIONS_TOTAL),
    metrics:create(counter, ?OPERATIONS_ERR),
    metrics:create(counter, ?OPERATIONS_SUC),

    %% Mian task
    job(Connection).

job(Connection) ->
    case meta_storage:get_random_market() of
        {MarketId, SelectionIds} ->
            SelectionId = generator:rand_nth(SelectionIds),
            update_odd(Connection, MarketId, SelectionId);
        _ -> emtpy_meta_storage
    end,


%    timer:sleep(?TASK_SLEEP),

    job(Connection).

update_odd(Connection, MarketId, SelectionId) ->
    Query = #{?ID => MarketId, <<"Selections.ID">> => SelectionId},
    Command = profiler:prof(?DEV_GEN_TIME, fun () -> #{<<"$set">> => #{ <<"Selections.$.Odds">> => generator:new_odd()}} end),
    Response = profiler:prof(?TIME, fun() -> mc_worker_api:update(Connection, ?MARKETINFO, Query, Command) end),
    case Response of
        {false, _} ->
            begin
                error_logger:error_msg("Can't update MarketInfo in module: ~p~n, response: ~p~n", [?MODULE, Response]),
                metrics:notify({?OPERATIONS_ERR, {inc, 1}})
            end;
        {true, #{ <<"writeErrors">> := WriteErrors}} ->
            begin
                error_logger:error_msg("Can't update MarketInfo in module: ~p~n, error: ~p~n", [?MODULE, WriteErrors]),
                metrics:notify({?OPERATIONS_ERR, {inc, 1}})
            end;
        {true,  #{ <<"n">> := N }} ->
            begin
                metrics:notify({?DOC_COUNT, N}),
                metrics:notify({?OPERATIONS_SUC, {inc, 1}})
            end
    end,
    metrics:notify({?OPERATIONS_TOTAL, {inc, 1}}),
    metrics:notify({?RATE, 1}).
