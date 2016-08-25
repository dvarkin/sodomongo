-module(meta_storage).
-author("eugeny").

-behaviour(gen_server).

-include("meta_storage.hrl").

%% API
-export([start_link/0]).
-export([insert_game/3, get_random_game/0, delete_game/1]).

%% gen_server callbacks
-export([init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3]).

-define(SERVER, {global, ?MODULE}).

-record(state, {game_info_tab, selection_tab}).

%%%===================================================================
%%% API
%%%===================================================================
insert_game(GameId, MarketIds, SelectionIds) ->
  gen_server:cast(?SERVER, {insert_game, GameId, MarketIds, SelectionIds}).

get_random_game() ->
  gen_server:call(?SERVER, get_random_game).

delete_game(GameId) ->
  gen_server:call(?SERVER, {delete_game, GameId}).


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
    Game_Info_Tab = ets:new(game_info_tab, [set, {keypos, 1}]),
    Selection_Tab = ets:new(selection_tab, [set, {keypos, 1}]),
    {ok, #state{game_info_tab = Game_Info_Tab, selection_tab = Selection_Tab}}.

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

handle_call(get_random_game, _From, #state{game_info_tab = Game_Info_Tab} = State) ->
    GameId = ets:first(Game_Info_Tab),
    {reply, GameId, State};
handle_call({delete_game, GameId}, _From, #state{game_info_tab = Game_Info_Tab, selection_tab = Selection_Tab} = State) ->
    MarketIds = ets:lookup(Game_Info_Tab, GameId),
    ets:delete(Game_Info_Tab, GameId),
    [ets:delete(Selection_Tab, MarketId) || MarketId <- MarketIds],
    {reply, MarketIds, State };
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
handle_cast({insert_game, GameId, MarketIds, Selections}, #state{game_info_tab=Game_Info_Tab, selection_tab=Selection_Tab} = State) ->
  ets:insert(Game_Info_Tab, {GameId, MarketIds}),
  [ets:insert(Selection_Tab, {MarketId, SelectionIds}) || {MarketId, SelectionIds}  <- Selections],
  {noreply, State};
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
