-module(delete_gameinfo_task).

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

-spec start(map()) -> {ok, pid()}.

start(Args) -> 
    gen_worker:start_link(?MODULE, Args).

-spec init(list()) -> {ok, term()}.

init(#{redis_conn_args := RedisConnArgs}) ->
    {ok, RedisConnection} = apply(eredis, start_link, RedisConnArgs),
    #state{redis_connection =  RedisConnection}.

-spec job({MasterConnection :: pid(), SlaveConnection :: pid()}, State :: term()) -> {ok, fun(), term()} 
                                                                                         | {ok, undefined, term()}.

job({Connection, _}, #state{redis_connection = RedisConnection} = State) ->
    job(meta_storage:delete_game(RedisConnection), Connection, State).

job(undefined, _, State) ->
    {ok, undefined, State};

job(#{ ?ID := Id}, Connection, State) ->
    {ok, delete(Connection, Id), State}.
    
%%%===================================================================
%%% Internal functions
%%%===================================================================

delete(Connection, GameId) ->
    fun() ->
            Response = mc_worker_api:delete(Connection, ?GAMEINFO, #{?ID => GameId}),
            parse_response(Response)
    end.

parse_response({{false, _}, _Data} = Response) ->
    #{status => error, response => Response};
parse_response({{true, #{ <<"writeErrors">> := _WriteErrors}}, _Data} = Response) -> 
    #{status => error, response => Response};
parse_response({{true, #{ <<"n">> := N }}, _Data} = Response) ->
    #{status => success, doc_count => N, response => Response};
parse_response({true, #{ <<"n">> := N }} = Response) ->
    #{status => success, doc_count => N, response => Response};
parse_response(Response) ->
    error_logger:error_msg("Unparsed response ~p", [Response]),
    #{status => error, response => Response}.

