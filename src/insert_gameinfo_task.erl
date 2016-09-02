-module(insert_gameinfo_task).
-include("generator.hrl").

%% API
-export([run/1, generate_data/0]).

-define(TASK_SLEEP, 1000).
-define(GAMEINFO_METRICS, <<"insert_gameinfo_task_metrics">>).
-define(MARKETINFO_METRICS, <<"insert_marketinfo_task_metrics">>).

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
-define(MARKETINFO_OPERATIONS_ERR, <<?MARKETINFO_OPERATIONS/binary, ".err">>).
-define(MARKETINFO_OPERATIONS_SUC, <<?MARKETINFO_OPERATIONS/binary, ".suc">>).


%%%===================================================================
%%% API
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

    %% MAIN TASK
    job(Connection).
    

job(Connection) ->
    {GameInfo, Markets,GameId, MarketIds, SelectionIds} = generate_data(),
    meta_storage:insert_game(GameId, MarketIds, SelectionIds),
    insert_gameinfo(Connection, GameInfo),
    insert_marketinfo(Connection, Markets),
    timer:sleep(?TASK_SLEEP),
    job(Connection).

%%%===================================================================
%%% Internal functions
%%%===================================================================

generate_data() ->
    {GameInfo, Markets} = generator:new_game_with_markets(),
    #{?ID := GameId} = GameInfo,
    MarketIds = [Id || #{?ID := Id} <- Markets],
    SelectionIds = lists:flatten([{MarketId, [SelectionId || #{?ID := SelectionId} <- Selections]} || #{?SELECTIONS := Selections, ?ID := MarketId} <- Markets]),
%    SelectionIds = [Id || #{?ID := Id} <- Selections],
    {GameInfo, Markets,GameId, MarketIds, SelectionIds}.

insert_gameinfo(Connection, GameInfo) ->
    metrics:notify({?GAMEINFO_RATE, 1}),
    metrics:notify({?GAMEINFO_OPERATIONS_TOTAL, {inc, 1}}),
    {Response, _} = profiler:prof(?GAMEINFO_TIME,
        fun() ->
            mc_worker_api:insert(Connection, ?GAMEINFO, GameInfo)
        end),
    case Response of
        {false, _} ->
            begin
                error_logger:error_msg("Can't insert GameInfo in module: ~p~n, response: ~p~n", [?MODULE, Response]),
                metrics:notify({?GAMEINFO_OPERATIONS_ERR, {inc, 1}})
            end;
        {true, #{ <<"writeErrors">> := WriteErrors}} ->
            begin
                error_logger:error_msg("Can't insert GameInfo in module: ~p~n, error: ~p~n", [?MODULE, WriteErrors]),
                metrics:notify({?GAMEINFO_OPERATIONS_ERR, {inc, 1}})
            end;
        {true,  #{ <<"n">> := N }} ->
            begin
                metrics:notify({?GAMEINFO_DOC_COUNT, N}),
                metrics:notify({?GAMEINFO_OPERATIONS_SUC, {inc, 1}})
            end
    end.
    

insert_marketinfo(_Connection, []) ->
    ok;
insert_marketinfo(Connection, [Market | Markets]) ->
    metrics:notify({?MARKETINFO_RATE, 1}),
    metrics:notify({?MARKETINFO_OPERATIONS_TOTAL, {inc, 1}}),
    {Response, _} = profiler:prof(?MARKETINFO_TIME, fun() -> mc_worker_api:insert(Connection, ?MARKETINFO, Market) end),
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
                metrics:notify({?MARKETINFO_DOC_COUNT, N}),
                metrics:notify({?MARKETINFO_OPERATIONS_TOTAL, {inc, 1}})
            end
    end,
    insert_marketinfo(Connection, Markets).
