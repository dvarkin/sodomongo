-module(delete_gameinfo_task).
-include("generator.hrl").

-export([run/1]).

-define(TASK_SLEEP, 1000).

-define(GAMEINFO_METRICS, <<(atom_to_binary(?MODULE, utf8))/binary, "_metrics">>).
-define(MARKETINFO_METRICS, <<"delete_marketinfo_task_metrics">>).

-define(GAMEINFO_RATE, <<?GAMEINFO_METRICS/binary, ".rate">>).
-define(GAMEINFO_TIME, <<?GAMEINFO_METRICS/binary, ".time">>).
-define(GAMEINFO_DOC_COUNT, <<?GAMEINFO_METRICS/binary, ".documents_count">>).
-define(GAMEINFO_OPERATIONS, <<?GAMEINFO_METRICS/binary, ".operations">>).
-define(GAMEINFO_OPERATIONS_TOTAL, <<?GAMEINFO_OPERATIONS/binary, ".total">>).
-define(GAMEINFO_OPERATIONS_ERR, <<?GAMEINFO_OPERATIONS/binary, ".err">>).
-define(GAMEINFO_OPERATIONS_SUC, <<?GAMEINFO_OPERATIONS/binary, ".suc">>).

-define(MARKETINFO_RATE, <<?MARKETINFO_METRICS/binary, ".rate">>).
-define(MARKETINFO_TIME, <<?MARKETINFO_METRICS/binary, ".time">>).
-define(MARKETINFO_DOC_COUNT, <<?MARKETINFO_METRICS/binary, ".documents_count">>).
-define(MARKETINFO_OPERATIONS, <<?MARKETINFO_METRICS/binary, ".operations">>).
-define(MARKETINFO_OPERATIONS_TOTAL, <<?MARKETINFO_OPERATIONS/binary, ".total">>).
-define(MARKETINFO_OPERATIONS_ERR, <<?MARKETINFO_OPERATIONS/binary, ".err">>).%%%===================================================================
-define(MARKETINFO_OPERATIONS_SUC, <<?MARKETINFO_OPERATIONS/binary, ".suc">>).%%% API
%%%===================================================================

run(Connection) ->
    
    %% init metrics
    metrics:create(meter, ?GAMEINFO_RATE),
    metrics:create(histogram, ?GAMEINFO_TIME),
    metrics:create(histogram, ?GAMEINFO_DOC_COUNT),
    metrics:create(counter, ?GAMEINFO_OPERATIONS_TOTAL),
    metrics:create(counter, ?GAMEINFO_OPERATIONS_ERR),
    metrics:create(counter, ?GAMEINFO_OPERATIONS_SUC),

    metrics:create(meter, ?MARKETINFO_RATE),
    metrics:create(histogram, ?MARKETINFO_TIME),
    metrics:create(histogram, ?MARKETINFO_DOC_COUNT),
    metrics:create(counter, ?GAMEINFO_OPERATIONS_TOTAL),
    metrics:create(counter, ?GAMEINFO_OPERATIONS_ERR),
    metrics:create(counter, ?GAMEINFO_OPERATIONS_SUC),

    %% Main task
    job(Connection).

job(Connection) ->
        case meta_storage:get_random_game() of
            GameId when is_integer(GameId) ->
                MarketIds = meta_storage:get_market_ids(GameId),
                meta_storage:delete_game(GameId),
                delete_gameinfo(Connection, GameId),
                delete_marketinfo(Connection, MarketIds);
            _ -> emtpy_meta_storage
        end,

    timer:sleep(?TASK_SLEEP),

    job(Connection).

delete_gameinfo(_Connection, _GameId) ->
    Response = profiler:prof(?GAMEINFO_TIME, fun() ->
        {true, ok}
%            mc_worker_api:delete(Connection, ?GAMEINFO, #{?ID => GameId})
                                             end),
    case Response of
        {false, _} ->
            begin
                error_logger:error_msg("Can't insert MarketInfo in module: ~p~n, response: ~p~n", [?MODULE, Response]),
                metrics:notify({?GAMEINFO_OPERATIONS_ERR, {inc, 1}})
            end;
        {true, #{ <<"writeErrors">> := WriteErrors}} ->
            begin
                error_logger:error_msg("Can't insert MarketInfo in module: ~p~n, error: ~p~n", [?MODULE, WriteErrors]),
                metrics:notify({?GAMEINFO_OPERATIONS_ERR, {inc, 1}})
            end;
        {true,  #{ <<"n">> := N }} ->
            begin
                metrics:notify({?GAMEINFO_DOC_COUNT, N}),
                metrics:notify({?GAMEINFO_OPERATIONS_SUC, {inc, 1}})
            end
    end,
    metrics:notify({?GAMEINFO_RATE, 1}),
    metrics:notify({?GAMEINFO_OPERATIONS_TOTAL, {inc, 1}}).

delete_marketinfo(_Connection, []) ->
    ok;
delete_marketinfo(Connection, [_MarketId | MarketIds]) ->
    Response = profiler:prof(?MARKETINFO_TIME, fun() ->
        %mc_worker_api:delete(Connection, ?MARKETINFO, #{?ID => MarketId})
        {true, [ok]}
        end),
    case Response of
        {false, _} ->
            begin
                error_logger:error_msg("Can't insert MarketInfo in module: ~p~n, response: ~p~n", [?MODULE, Response]),
                metrics:notify({?MARKETINFO_OPERATIONS_ERR, {inc, 1}})
            end;
        {true, #{ <<"writeErrors">> := WriteErrors}} ->
            begin
                error_logger:error_msg("Can't insert MarketInfo in module: ~p~n, error: ~p~n", [?MODULE, WriteErrors]),
                metrics:notify({?MARKETINFO_OPERATIONS_ERR, {inc, 1}})
            end;
        {true,  #{ <<"n">> := N }} ->
            begin
                metrics:notify({?GAMEINFO_DOC_COUNT, N}),
                metrics:notify({?GAMEINFO_OPERATIONS_SUC, {inc, 1}})
            end
    end,
    metrics:notify({?MARKETINFO_RATE, 1}),
    metrics:notify({?MARKETINFO_OPERATIONS_TOTAL, {inc, 1}}),
    delete_gameinfo(Connection, MarketIds).
