-module(update_odd_task).
-include("generator.hrl").
-include("profiler.hrl").

-export([run/1]).

-define(TASK_SLEEP, 10).
-define(TASK, <<(atom_to_binary(?MODULE, utf8))/binary, "_metrics">>).
-define(RATE, <<?TASK/binary, ".rate">>).
-define(TIME, <<?TASK/binary, ".time">>).

%%%===================================================================
%%% API
%%%===================================================================

%% create_indexes(Connection) ->
%%     mc_worker_api:ensure_index(Connection, ?MARKETINFO, #{<<"key">> => {?ID, <<"hashed">>}}),
%%     mc_worker_api:ensure_index(Connection, ?MARKETINFO, #{<<"key">> => {<<"Selections.ID">>,1}}).



run(Connection) ->
    
    %% init metrics
    metrics:create(meter, ?RATE),
    metrics:create(histogram, ?TIME),

    %% Mian task
    job(Connection).

job(Connection) ->
    case meta_storage:get_random_market() of
        {MarketId, SelectionIds} ->
            SelectionId = generator:rand_nth(SelectionIds),
            update_odd(Connection, MarketId, SelectionId);
        _ -> emtpy_meta_storage
    end,


%    timer:sleep(?TASK_SLEEP),

    job(Connection).

update_odd(Connection, MarketId, SelectionId) ->
    Query = #{?ID => MarketId, <<"Selections.ID">> => SelectionId},
    Command = #{<<"$set">> => #{ <<"Selections.$.Odds">> => generator:new_odd()}},
    ?GPROF_TIME_METRIC(mc_worker_api:update(Connection, ?MARKETINFO, Query, Command), ?TIME),
    metrics:notify({?RATE, 1}).
