-module(insert_marketinfo_task).

-behaviour(gen_worker).

-include("generator.hrl").

%% API
-export([start/3, start/0]).


%% gen_worker behaviour API

-export([init_metrics/0, job/2, init/1]).

init_metrics() ->
    gen_worker:init_metrics(?MODULE).

%%%===================================================================
%%% API
%%%===================================================================

start() ->
    {ok, RawArgs} = application:get_env(sodomongo, mongo_connection),
    start(RawArgs, 5000, 1000).

-spec start(ConnectionArgs :: list(), Time :: pos_integer(), SleepTimer :: pos_integer() | undefined) -> {ok, pid()}.    

start(ConnectionArgs, Time, SleepTimer) -> 
    gen_worker:start(?MODULE, ConnectionArgs, Time, SleepTimer).

-spec init(list()) -> {ok, term()}.

init(_Init_Args) ->
    {ok, undefined_state}.


-spec job({MasterConnection :: pid(), SlaveConnection :: pid()}, State :: term()) -> {ok, fun(), term()} 
                                                                                         | {ok, undefined, term()}.
job({Connection, _}, State) ->
    case meta_storage:get_random_market() of 
        undefined ->
            {ok, undefined, State};
        Market ->
            {ok, insert(Connection, Market), State}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

insert(Connection, Data) ->
    fun() ->
            Response =  mc_worker_api:insert(Connection, ?MARKETINFO, Data),
            parse_response(Response)
    end.

parse_response({{false, _}, _Data} = Response) ->
    #{status => error, response => Response};
parse_response({{true, #{ <<"writeErrors">> := _WriteErrors}}, _Data} = Response) -> 
    #{status => error, response => Response};
parse_response({{true, #{ <<"n">> := N }}, _Data} = Response) ->
    #{status => success, doc_count => N, response => Response};
parse_response(Response) ->
    error_logger:error_msg("Unparsed response ~p", [Response]),
    #{status => error, response => Response}.

