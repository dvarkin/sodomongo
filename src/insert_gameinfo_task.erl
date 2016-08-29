-module(insert_gameinfo_task).
-include("generator.hrl").
-include("profiler.hrl").

%% API
-export([run/1, generate_data/0]).

-define(TASK_SLEEP, 1000).
-define(GAMEINFO_METRICS, <<"insert_gameinfo_task_metrics">>).
-define(MARKETINFO_METRICS, <<"insert_marketinfo_task_metrics">>).

-define(GAMEINFO_RATE, <<?GAMEINFO_METRICS/binary, ".rate">>).
-define(GAMEINFO_TIME, <<?GAMEINFO_METRICS/binary, ".time">>).

-define(MARKETINFO_RATE, <<?MARKETINFO_METRICS/binary, ".rate">>).
-define(MARKETINFO_TIME, <<?MARKETINFO_METRICS/binary, ".time">>).


%%%===================================================================
%%% API
%%%===================================================================

run(Connection) -> 

    %% init metrics

    metrics:create(meter, ?GAMEINFO_RATE),
    metrics:create(histogram, ?GAMEINFO_TIME),
    metrics:create(meter, ?MARKETINFO_RATE),
    metrics:create(histogram, ?MARKETINFO_TIME),

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
    ?GPROF_TIME_METRIC(mc_worker_api:insert(Connection, ?GAMEINFO, GameInfo), ?GAMEINFO_TIME).
    

insert_marketinfo(_Connection, []) ->
    ok;
insert_marketinfo(Connection, [Market | Markets]) ->
    metrics:notify({?MARKETINFO_RATE, 1}),
    ?GPROF_TIME_METRIC(mc_worker_api:insert(Connection, ?MARKETINFO, Market), ?MARKETINFO_TIME),
    insert_marketinfo(Connection, Markets).
