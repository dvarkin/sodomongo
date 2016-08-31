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

-define(MARKETINFO_RATE, <<?MARKETINFO_METRICS/binary, ".rate">>).
-define(MARKETINFO_TIME, <<?MARKETINFO_METRICS/binary, ".time">>).
-define(MARKETINFO_DOC_COUNT, <<?MARKETINFO_METRICS/binary, ".documents_count">>).



%%%===================================================================
%%% API
%%%===================================================================

run(Connection) -> 

    %% init metrics

    metrics:create(meter, ?GAMEINFO_RATE),
    metrics:create(histogram, ?GAMEINFO_TIME),
    metrics:create(histogram, ?GAMEINFO_DOC_COUNT),
    metrics:create(meter, ?MARKETINFO_RATE),
    metrics:create(histogram, ?MARKETINFO_TIME),
    metrics:create(histogram, ?MARKETINFO_DOC_COUNT),

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
    {Response, _} = profiler:prof(?GAMEINFO_TIME, fun() -> mc_worker_api:insert(Connection, ?GAMEINFO, GameInfo) end),
    case Response of
        {false, _} ->
            error_logger:error_msg("Can't insert GameInfo in module: ~p~n, response: ~p~n", [?MODULE, Response]);
        {true, #{ <<"writeErrors">> := WriteErrors}} ->
            error_logger:error_msg("Can't insert GameInfo in module: ~p~n, error: ~p~n", [?MODULE, WriteErrors]);
        {true,  #{ <<"n">> := N }}
            -> metrics:notify({?GAMEINFO_DOC_COUNT, N})
    end.
    

insert_marketinfo(_Connection, []) ->
    ok;
insert_marketinfo(Connection, [Market | Markets]) ->
    metrics:notify({?MARKETINFO_RATE, 1}),
    {Response, _} = profiler:prof(?MARKETINFO_TIME, fun() -> mc_worker_api:insert(Connection, ?MARKETINFO, Market) end),
    case Response of
        {false, _} ->
            error_logger:error_msg("Can't insert MarketInfo in module: ~p~n, response: ~p~n", [?MODULE, Response]);
        {true, #{ <<"writeErrors">> := WriteErrors}} ->
            error_logger:error_msg("Can't insert MarketInfo in module: ~p~n, error: ~p~n", [?MODULE, WriteErrors]);
        {true,  #{ <<"n">> := N }}
            -> metrics:notify({?GAMEINFO_DOC_COUNT, N})
    end,
    insert_marketinfo(Connection, Markets).
