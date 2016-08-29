-module(update_odd_task).
-include("generator.hrl").

-export([run/1]).

-define(TASK_SLEEP, 10).
-define(MARKET_INFO_METRIC, <<"update_market_info">>).


%%%===================================================================
%%% API
%%%===================================================================

run(Connection) ->
    
    %% init metrics

    metrics:create(meter, ?MARKET_INFO_METRIC),
    
    %% Mian task
    job(Connection).

job(Connection) ->
        case meta_storage:get_random_market() of
            {MarketId, SelectionIds} ->
                SelectionId = generator:rand_nth(SelectionIds),
                Query = #{?ID => MarketId, <<"Selections.ID">> => SelectionId},
                Command = #{<<"$set">> => #{ <<"Selections.$.Odds">> => generator:new_odd()}},
%                error_logger:info_msg("Query ~p Command ~p", [Query, Command]),
                mc_worker_api:update(Connection, <<"marketinfo">>, Query, Command),
                metrics:notify({?MARKET_INFO_METRIC, 1});
            _ -> emtpy_meta_storage
        end,



%    timer:sleep(?TASK_SLEEP),

    job(Connection).







