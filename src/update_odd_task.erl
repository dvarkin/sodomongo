-module(update_odd_task).
-include("generator.hrl").

-export([run/1]).

-define(TASK_SLEEP, 10).
-define(TASK, <<(atom_to_binary(?MODULE, utf8))/binary, "_metrics">>).
-define(RATE, <<?TASK/binary, ".rate">>).
-define(TIME, <<?TASK/binary, ".time">>).

%%%===================================================================
%%% API
%%%===================================================================

create_indexes(Connection) ->
    mc_worker_api:ensure_index(Connection, <<"marketinfo">>, #{<<"key">> => {?ID, <<"hashed">>}}),
    mc_worker_api:ensure_index(Connection, <<"marketinfo">>, #{<<"key">> => {<<"Selections.ID">>,1}}).



run(Connection) ->
    
    %% init metrics
    metrics:create(meter, ?RATE),
    metrics:create(gauge, ?TIME),

    %% Mian task
    create_indexes(Connection),
    job(Connection).

job(Connection) ->
        case meta_storage:get_random_market() of
            {MarketId, SelectionIds} ->
                SelectionId = generator:rand_nth(SelectionIds),
                Query = #{?ID => MarketId, <<"Selections.ID">> => SelectionId},
                Command = #{<<"$set">> => #{ <<"Selections.$.Odds">> => generator:new_odd()}},
                {Time, _Value} = timer:tc(
                                   fun() ->
                                           mc_worker_api:update(Connection, <<"marketinfo">>, Query, Command)
                                   end),
                metrics:notify({?RATE, 1}),
                metrics:notify({?TIME, Time});
            _ -> emtpy_meta_storage
        end,



%    timer:sleep(?TASK_SLEEP),

    job(Connection).







