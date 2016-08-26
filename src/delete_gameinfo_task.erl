-module(delete_gameinfo_task).
-include("generator.hrl").

-export([run/1]).

-define(TASK_SLEEP, 1000).
-define(GAME_INFO_METRIC, <<"delete_game_info">>).
-define(MARKET_INFO_METRIC, <<"delete_market_info">>).


%%%===================================================================
%%% API
%%%===================================================================

run(Connection) ->
    
    %% init metrics

    metrics:create(meter, ?GAME_INFO_METRIC),
    metrics:create(meter, ?MARKET_INFO_METRIC),
    
    %% Mian task
    job(Connection).

job(Connection) ->
        case meta_storage:get_random_game() of
            GameId when is_binary(GameId) ->
            
                MarketIds = meta_storage:get_market_ids(GameId),
                mc_worker_api:delete(Connection, <<"gameinfo">>, #{?ID => GameId}),
                [mc_worker_api:delete(Connection, <<"marketinfo">>, #{?ID => MarketId}) || MarketId <- MarketIds],
                meta_storage:delete_game(GameId),
                metrics:notify({?GAME_INFO_METRIC, 1}),
                metrics:notify({?MARKET_INFO_METRIC, length(MarketIds)});
            _ -> emtpy_meta_storage
        end,



    timer:sleep(?TASK_SLEEP),

    job(Connection).







