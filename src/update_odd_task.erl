-module(update_odd_task).
-include("generator.hrl").

-export([run/1]).

-define(TASK_SLEEP, 10).
-define(TASK, <<(atom_to_binary(?MODULE, utf8))/binary, "_metrics">>).
-define(RATE, <<?TASK/binary, ".rate">>).
-define(TIME, <<?TASK/binary, ".time">>).
-define(DOC_COUNT, <<?TASK/binary>>, ".documents_count").

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
    metrics:create(histogram, ?DOC_COUNT),

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
    Response = profiler:prof(?TIME, fun() -> mc_worker_api:update(Connection, ?MARKETINFO, Query, Command) end),
    case Response of
        {false, _} ->
            error_logger:error_msg("Can't insert MarketInfo in module: ~p~n, response: ~p~n", [?MODULE, Response]);
        {true, #{ <<"writeErrors">> := WriteErrors}} ->
            error_logger:error_msg("Can't insert MarketInfo in module: ~p~n, error: ~p~n", [?MODULE, WriteErrors]);
        {true,  #{ <<"n">> := N }}
            -> metrics:notify({?DOC_COUNT, N})
    end,
    metrics:notify({?RATE, 1}).
