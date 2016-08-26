-module(insert_gameinfo_task).
-include("generator.hrl").

%% API
-export([run/1, generate_data/0]).

-define(TASK_SLEEP, 1000).
-define(GAME_INFO_METRIC, <<"insert_game_info">>).
-define(MARKET_INFO_METRIC, <<"insert_market_info">>).

%%%===================================================================
%%% API
%%%===================================================================

run(Connection) -> 

    %% init metrics

    metrics:create(meter, ?GAME_INFO_METRIC),
    metrics:create(meter, ?MARKET_INFO_METRIC),

    %% MAIN TASK
    job(Connection).
    

job(Connection) ->

    {GameInfo, Markets,GameId, MarketIds, SelectionIds} = generate_data(),

    meta_storage:insert_game(GameId, MarketIds, SelectionIds),
    mc_worker_api:insert(Connection, <<"gameinfo">>, GameInfo),
    [mc_worker_api:insert(Connection, <<"marketinfo">>, Market) || Market <- Markets],
    
    metrics:notify({?GAME_INFO_METRIC, 1}),
    metrics:notify({?MARKET_INFO_METRIC, length(Markets)}),
    
    timer:sleep(?TASK_SLEEP),
    
    job(Connection).


%%--------------------------------------------------------------------
%% @doc
%% @spec
%% @end
%%--------------------------------------------------------------------

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
