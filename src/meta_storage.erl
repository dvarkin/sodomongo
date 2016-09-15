-module(meta_storage).
-author("eugeny").

-behaviour(gen_server).

-include("meta_storage.hrl").
-include("generator.hrl").

%% API
-export([start_link/0]).
-export([get_pool/0,
    get_storage/0,
    get_random_game/0,
    delete_game/1,
    delete_market/1,
    get_market_ids/1,
    get_selections/1,
    get_random_market_id/0,
    pull_all_markets/0,
    new_game_with_markets/0]).

%% gen_server callbacks
-export([init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3]).

-define(SERVER, {global, ?MODULE}).
-define(POOL_SIZE, 1).

-record(state, {game_info_tab = #{}, selection_tab = #{}, markets = [], games_pool = #{}, games_storage = #{}}).

%%%===================================================================
%%% API
%%%===================================================================
get_pool() ->
    gen_server:call(?SERVER, get_pool).

get_storage() ->
    gen_server:call(?SERVER, get_storage).

new_game_with_markets() ->
    gen_server:call(?SERVER, new_game_with_markets).

get_random_game() ->
  gen_server:call(?SERVER, get_random_game).

get_random_market_id() ->
    gen_server:call(?SERVER, get_random_market_id).

pull_all_markets() ->
    gen_server:call(?SERVER, pull_all_markets).

get_market_ids(GameId) ->
    gen_server:call(?SERVER, {get_market_ids, GameId}).

get_selections(MarketId) ->
    gen_server:call(?SERVER, {get_selections, MarketId}).

delete_game(GameId) ->
  gen_server:cast(?SERVER, {delete_game, GameId}).

delete_market(MarketId) ->
  gen_server:cast(?SERVER, {delete_market, MarketId}).

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @end
%%--------------------------------------------------------------------
-spec(start_link() ->
  {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
  gen_server:start_link(?SERVER, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
-spec(init(Args :: term()) ->
  {ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term()} | ignore).
init([]) ->
    GamesWithMarkets = [generator:new_game_with_markets() || _ <- lists:seq(1, ?POOL_SIZE)],
    GamesWithMarketsPool = [{GameId, {Game, Markets}} || {#{?ID := GameId} = Game, Markets} <- GamesWithMarkets],
    GamesWithMarketsPoolMap = maps:from_list(GamesWithMarketsPool),
    {ok, #state{games_pool = GamesWithMarketsPoolMap}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
	State :: #state{}) ->
  {reply, Reply :: term(), NewState :: #state{}} |
  {reply, Reply :: term(), NewState :: #state{}, timeout() | hibernate} |
  {noreply, NewState :: #state{}} |
  {noreply, NewState :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term(), Reply :: term(), NewState :: #state{}} |
  {stop, Reason :: term(), NewState :: #state{}}).

handle_call(get_pool, _From, State) ->
    {reply, State#state.games_pool, State};
handle_call(get_storage, _From, State) ->
    {reply, State#state.games_storage, State};
handle_call(new_game_with_markets, _From, #state{games_pool = Pool} = State) ->
    {GameWithMarkets, NewState} = insert_pool_item(util:rand_kv(Pool), State),
    {reply, GameWithMarkets, NewState};
handle_call(get_random_game, _From, #state{game_info_tab = Game_Info_Tab} = State) ->
    GameId = util:rand_nth(maps:keys(Game_Info_Tab)),
    {reply, GameId, State};
handle_call(pull_all_markets, _From, #state{markets = Markets} = State) ->
    {reply, Markets, State#state{markets = []}};
handle_call({get_market_ids, GameId}, _From, #state{game_info_tab = Game_Info_Tab} = State) ->
    MarketIds = maps:get(GameId, Game_Info_Tab),
    {reply, MarketIds, State };
handle_call({get_selections, MarketId}, _From, #state{selection_tab = Market_Info_Tab} = State) ->
    Selections = maps:get(MarketId, Market_Info_Tab),
    {reply, Selections, State};
handle_call(get_random_market_id, _From, #state{selection_tab = Market_Info_Tab} = State) ->
    Reply = case util:rand_nth(maps:keys(Market_Info_Tab)) of
                undefined ->
                    undefined;
                MarketId ->
                    Selections = maps:get(MarketId, Market_Info_Tab),
                    {MarketId, Selections}
            end,
    {reply, Reply,State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_cast(Request :: term(), State :: #state{}) ->
  {noreply, NewState :: #state{}} |
  {noreply, NewState :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #state{}}).
handle_cast({delete_game, GameId}, #state{game_info_tab = Game_Info_Tab, games_pool = Pool, games_storage = Storage} = State) ->
    Game_Info_Tab1 = maps:remove(GameId, Game_Info_Tab),
    PoolEntry = maps:get(GameId, Storage),
    Storage1 = maps:without([GameId], Storage),
    Pool1 = maps:put(GameId, PoolEntry, Pool),
    {noreply, State#state{game_info_tab = Game_Info_Tab1, games_pool = Pool1, games_storage = Storage1}};

handle_cast({delete_market, MarketId}, #state{selection_tab = Selection_Tab} = State) ->
    Selection_Tab1 = maps:remove(MarketId, Selection_Tab),
    {noreply, State#state{selection_tab = Selection_Tab1}};

handle_cast(_Request, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%									 {noreply, State, Timeout} |
%%									 {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
-spec(handle_info(Info :: timeout() | term(), State :: #state{}) ->
  {noreply, NewState :: #state{}} |
  {noreply, NewState :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #state{}}).
handle_info(_Info, State) ->
  {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
-spec(terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
	State :: #state{}) -> term()).
terminate(_Reason, _State) ->
  ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
-spec(code_change(OldVsn :: term() | {down, term()}, State :: #state{},
	Extra :: term()) ->
  {ok, NewState :: #state{}} | {error, Reason :: term()}).
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
insert_pool_item(undefined, State) ->
    {undefined, State};
insert_pool_item(PoolItem, State) ->
    {GameId, {GameInfo, Markets}} = PoolItem,
    MarketIds = [Id || #{?ID := Id} <- Markets],
    SelectionIds = lists:flatten([{MarketId, [SelectionId || #{?ID := SelectionId} <- Selections]} || #{?SELECTIONS := Selections, ?ID := MarketId} <- Markets]),
    NewState = insert_game(GameId, MarketIds, SelectionIds, Markets, State),
    {{GameInfo, Markets}, NewState}.

insert_game(GameId, MarketIds, SelectionIds, NewMarkets, #state{game_info_tab = GameInfoTab, selection_tab = SelectionTab, markets = Markets, games_pool = Pool, games_storage = Storage} = State) ->
    S1 = maps:from_list(SelectionIds),
    Selection_Tab1 = maps:merge(S1, SelectionTab),
    Game_Info_Tab1 = maps:put(GameId, MarketIds, GameInfoTab),
    PoolEntry = maps:get(GameId, Pool),
    Pool1 = maps:without([GameId], Pool),
    Storage1 = maps:put(GameId, PoolEntry, Storage),
    State#state{game_info_tab = Game_Info_Tab1, selection_tab = Selection_Tab1, markets = lists:merge(NewMarkets, Markets), games_pool = Pool1, games_storage = Storage1}.

% meta_storage:start_link(), meta_storage:new_game_with_markets(), meta_storage:new_game_with_markets(), ok.
% maps:size(meta_storage:get_pool()).