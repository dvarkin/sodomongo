-module(query_non_zero_odds_markets).
-author("eugeny").

%% API
-export([run/1]).

-define(TASK_SLEEP, 1).
-define(TASK, <<(atom_to_binary(?MODULE, utf8))/binary, "_metrics">>).
-define(RATE, <<?TASK/binary, ".rate">>).
-define(TIME, <<?TASK/binary, ".time">>).
-define(DOC_COUNT, <<?TASK/binary, ".documents_count">>).

job(Connection) ->
    Query = #{<<"Selections.Odds">> => #{<<"$ne">> => 0}},
    Collection = <<"marketinfo">>,
    Result = profiler:prof(
        ?TIME,
        fun() ->
            Cursor = mc_worker_api:find(
                Connection,
                Collection,
                Query
            ),
            mc_cursor:rest(Cursor)
        end),

    if
        Result == error -> error_logger:error_msg("Can't fetch response: ~p~n", [?MODULE]);
        true  ->  metrics:notify({?DOC_COUNT, length(Result)})
    end,

    metrics:notify({?RATE, 1}),
    timer:sleep(?TASK_SLEEP),
    job(Connection).

run(Connection) ->
    metrics:create(meter, ?RATE),
    metrics:create(histogram, ?TIME),
    metrics:create(histogram, ?DOC_COUNT),
    job(Connection).