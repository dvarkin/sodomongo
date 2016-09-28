-module(delete_gameinfo_task).

-behaviour(gen_worker).

-include("../generator.hrl").

%% API

-export([start/1]).

-record(state, {redis_connection = undefined :: pid() | undefined}).

%% gen_worker behaviour API

-export([init_metrics/0, job/2, init/1]).

init_metrics() ->
    gen_worker:init_metrics(?MODULE).

%%%===================================================================
%%% API
%%%===================================================================

-spec start(gen_worker:worker_args()) -> 'ignore' | {'error',_} | {'ok',pid()}.

start(Args) -> 
    gen_worker:start_link(?MODULE, Args).

-spec init(Args :: gen_worker:worker_args()) -> #state{redis_connection :: pid() | undefined}.

init(#{redis_conn_args := RedisConnArgs, module := ?MODULE, mongo_conn_args := _, sleep := _, time := _} = _Args) ->
    {ok, RedisConnection} = apply(eredis, start_link, RedisConnArgs),
    #state{redis_connection =  RedisConnection}.

-spec job({MasterConnection :: mongo:master_connection(), SlaveConnection  :: mongo:master_connection()}, 
          State :: term()) 
         -> gen_worker:job_response().

job({Connection, _}, #state{redis_connection = RedisConnection} = State) ->
    job(meta_storage:delete_game(RedisConnection), Connection, State).

-spec job(Id :: pos_integer() | undefined, 
          Connection ::  mongo:master_connection() | mongo:master_connection(), 
          State :: term()) 
         -> gen_worker:job_response().

job(undefined, _, State) ->
    {ok, undefined, State};

job(#{?ID := Id}, Connection, State) ->
    {ok, delete(Connection, Id), State}.
    
%%%===================================================================
%%% Internal functions
%%%===================================================================

%%-spec delete(mongo:master_connection() | mongo:secondary_connection(), pos_integer()) ->  fun(() -> none()).

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

