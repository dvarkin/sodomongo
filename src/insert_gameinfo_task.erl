-module(insert_gameinfo_task).
-include("generator.hrl").

%% API
-export([run/1]).

-define(TASK_SLEEP, 1000).

%%%===================================================================
%%% API
%%%===================================================================


run(Connection) ->
  {GameInfo, Markets} = generator:new_game_with_markets(),

  #{?ID := GameId} = GameInfo,
  MarketIds = [Id || #{?ID := Id} <- Markets],
  Selections = [Selections || #{?SELECTIONS := Selections} <- Markets],
  SelectionIds = lists:flatten([Id || #{?ID := Id} <- Selections]),

  meta_storage:insert_game(GameId, MarketIds, SelectionIds),

  mc_worker_api:insert(Connection, <<"gameinfo">>, GameInfo),
  [mc_worker_api:insert(Connection, <<"marketinfo">>, Market) || Market <- Markets],

  metrics:notify({<<"insert_name_info">>, 1}),
  metrics:notify({<<"insert_market_info">>, length(Markets)}),

  timer:sleep(?TASK_SLEEP),

  run(Connection).


%%--------------------------------------------------------------------
%% @doc
%% @spec
%% @end
%%--------------------------------------------------------------------

%%%===================================================================
%%% Internal functions
%%%===================================================================
