-module(insert_gameinfo_task).

-behaviour(gen_worker).

-include("../generator.hrl").

%% API
-export([start/1, start/0]).

-record(state, {redis_connection}).

%% gen_worker behaviour API

-export([init_metrics/0, job/2, init/1]).

init_metrics() ->
    gen_worker:init_metrics(?MODULE).

%%%===================================================================
%%% API
%%%===================================================================

start() ->
    ConnArgs = mongo:conn_args(),
    {ok, RedisArgs} = application:get_env(sodomongo, redis_connection),
    start(#{mongo_conn_args => ConnArgs, time => 5000, sleep => 1000, redis_conn_args => RedisArgs}).


-spec start(Args :: map()) -> {ok, pid()}.    

start(Args) -> 
    gen_worker:start_link(?MODULE, Args).

-spec init(map()) -> {ok, term()}.

init(#{redis_conn_args := RedisConnArgs} = _Args) ->
    {ok, RedisConnection} = apply(eredis, start_link, RedisConnArgs),
    #state{redis_connection = RedisConnection}.

-spec job({MasterConnection :: pid(), SlaveConnection :: pid()}, State :: term()) -> {ok, fun(), term()} 
                                                                                         | {ok, undefined, term()}.

job({Connection, _}, #state{redis_connection = RedisConnection} = State) ->
    {GameInfo, Markets, _, MarketIds} = generate_data(),
    meta_storage:insert_game(RedisConnection, GameInfo, MarketIds, Markets),
    {ok, insert(Connection, GameInfo), State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

insert(Connection, GameInfo) ->
    fun() ->
            Response =  mc_worker_api:insert(Connection, ?GAMEINFO, GameInfo),
            parse_response(Response)
    end.

generate_data() ->
    {GameInfo, Markets} = generator:new_game_with_markets(),
    #{?ID := GameId} = GameInfo,
    MarketIds = [Id || #{?ID := Id} <- Markets],
    {GameInfo, Markets, GameId, MarketIds}.

parse_response({{false, _}, _Data} = Response) ->
    #{status => error, response => Response};
parse_response({{true, #{ <<"writeErrors">> := _WriteErrors}}, _Data} = Response) -> 
    #{status => error, response => Response};
parse_response({{true, #{ <<"n">> := N }}, _Data} = Response) ->
    #{status => success, doc_count => N, response => Response};
parse_response(Response) ->
    error_logger:error_msg("Unparsed response ~p", [Response]),
    #{status => error, response => Response}.


