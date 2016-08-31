-module(delete_gameinfo_task).
-include("generator.hrl").

-export([run/1]).

-define(TASK_SLEEP, 1000).

-define(GAMEINFO_METRICS, <<(atom_to_binary(?MODULE, utf8))/binary, "_metrics">>).
-define(MARKETINFO_METRICS, <<"delete_marketinfo_task_metrics">>).

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
    metrics:create(meter, ?MARKETINFO_RATE),
    metrics:create(histogram, ?MARKETINFO_TIME),

    %% Mian task
    job(Connection).

job(Connection) ->
        case meta_storage:get_random_game() of
            GameId when is_integer(GameId) ->
                MarketIds = meta_storage:get_market_ids(GameId),
                meta_storage:delete_game(GameId),
                delete_gameinfo(Connection, GameId),
                delete_marketinfo(Connection, MarketIds);
            _ -> emtpy_meta_storage
        end,

    timer:sleep(?TASK_SLEEP),

    job(Connection).

delete_gameinfo(Connection, GameId) ->
    Response = profiler:prof(?GAMEINFO_TIME, fun() -> mc_worker_api:delete(Connection, ?GAMEINFO, #{?ID => GameId}) end),
    case Response of
        {false, _} ->
            error_logger:error_msg("Can't insert MarketInfo in module: ~p~n, response: ~p~n", [?MODULE, Response]);
        {true, #{ <<"writeErrors">> := WriteErrors}} ->
            error_logger:error_msg("Can't insert MarketInfo in module: ~p~n, error: ~p~n", [?MODULE, WriteErrors]);
        {true,  #{ <<"n">> := N }}
            -> metrics:notify({?GAMEINFO_DOC_COUNT, N})
    end,
    metrics:notify({?GAMEINFO_RATE, 1}).

delete_marketinfo(_Connection, []) ->
    ok;
delete_marketinfo(Connection, [MarketId | MarketIds]) ->
    Response = profiler:prof(?MARKETINFO_TIME, fun() -> mc_worker_api:delete(Connection, ?MARKETINFO, #{?ID => MarketId}) end),
    case Response of
        {false, _} ->
            error_logger:error_msg("Can't insert MarketInfo in module: ~p~n, response: ~p~n", [?MODULE, Response]);
        {true, #{ <<"writeErrors">> := WriteErrors}} ->
            error_logger:error_msg("Can't insert MarketInfo in module: ~p~n, error: ~p~n", [?MODULE, WriteErrors]);
        {true,  #{ <<"n">> := N }}
            -> metrics:notify({?GAMEINFO_DOC_COUNT, N})
    end,
    metrics:notify({?MARKETINFO_RATE, 1}),
    delete_gameinfo(Connection, MarketIds).
