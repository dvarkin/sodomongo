-module(meta_storage).

-export([insert_game/5, 
         insert_market/3,
         delete_game/2,
         delete_market/2,
         pull_market/1
        ]).

insert_game(C, GameId, MarketIds, _SelectionIds, Markets) ->
    eredis:q(C, ["LPUSH", "markets", Markets]),
    eredis:q(C, ["HSET", "gameinfo", GameId, MarketIds]).

insert_market(C, MarketId, Selections) ->
    eredis:q(C, ["HSET", "marketinfo", MarketId, Selections]).

delete_game(C, GameId) ->
    eredis:q(C, ["HDEL", "gameinfo", GameId]).

delete_market(C, MarketId) ->
    eredis:q(C, ["HDEL", "marketinfo", MarketId]).
    
pull_market(C) ->
    eredis:q(C, ["RPOP", "markets"]).

    
%% get_random_game() ->


%% get_random_market_id() ->




%% get_market_ids(GameId) ->


%% get_selections(MarketId) ->




%% handle_call(get_random_game, _From, #state{game_info_tab = Game_Info_Tab} = State) ->
%%     GameId = util:rand_nth(maps:keys(Game_Info_Tab)),
%%     {reply, GameId, State};
%% handle_call(pull_all_markets, _From, #state{markets = Markets} = State) ->
%%     {reply, Markets, State#state{markets = []}};
%% handle_call({get_market_ids, GameId}, _From, #state{game_info_tab = Game_Info_Tab} = State) ->
%%     MarketIds = maps:get(GameId, Game_Info_Tab),
%%     {reply, MarketIds, State };
%% handle_call({get_selections, MarketId}, _From, #state{selection_tab = Market_Info_Tab} = State) ->
%%     Selections = maps:get(MarketId, Market_Info_Tab),
%%     {reply, Selections, State};
%% handle_call(get_random_market_id, _From, #state{selection_tab = Market_Info_Tab} = State) ->
%%     Reply = case util:rand_nth(maps:keys(Market_Info_Tab)) of
%%                 undefined ->
%%                     undefined;
%%                 MarketId ->
%%                     Selections = maps:get(MarketId, Market_Info_Tab),
%%                     {MarketId, Selections}
%%             end,
%%     {reply, Reply,State};
%% handle_cast({insert_game, GameId, MarketIds, Selections, NewMarkets}, #state{game_info_tab=Game_Info_Tab, selection_tab=Selection_Tab, markets = Markets} = State) ->
%%     S1 = maps:from_list(Selections),
%%     Selection_Tab1 = maps:merge(S1, Selection_Tab),
%%     Game_Info_Tab1 = maps:put(GameId, MarketIds, Game_Info_Tab),
%%     {noreply, State#state{game_info_tab = Game_Info_Tab1, selection_tab = Selection_Tab1, markets = lists:merge(NewMarkets, Markets)}};

%% handle_cast({delete_game, GameId}, #state{game_info_tab = Game_Info_Tab} = State) ->
%%     Game_Info_Tab1 = maps:remove(GameId, Game_Info_Tab),
%%     {noreply, State#state{game_info_tab = Game_Info_Tab1}};

%% handle_cast({delete_market, MarketId}, #state{selection_tab = Selection_Tab} = State) ->
%%     Selection_Tab1 = maps:remove(MarketId, Selection_Tab),
%%     {noreply, State#state{selection_tab = Selection_Tab1}};


