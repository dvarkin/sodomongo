-module(delete_gameinfo_task).
-include("generator.hrl").

-export([run/1]).

-define(TASK_SLEEP, 1000).

%%%===================================================================
%%% API
%%%===================================================================


run(Connection) ->
  GameId = ets:first(gameinfo),
  [{_, MarketIds}|_]= ets:lookup(gameinfo, GameId),
  ets:delete(gameinfo, GameId),
  [ets:delete(marketinfo, MarketId) || MarketId <- MarketIds],

  mc_worker_api:delete(Connection, <<"gameinfo">>, #{?ID => GameId}),
  [mc_worker_api:delete(Connection, <<"marketinfo">>, #{?ID => MarketId}) || MarketId <- MarketIds],

  metrics:notify({<<"delete_name_info">>, 1}),
  metrics:notify({<<"delete_market_info", length(MarketIds)>>}),

  timer:sleep(?TASK_SLEEP),

  run(Connection).







