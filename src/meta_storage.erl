-module(meta_storage).

-define(GAMEINFO, "gameinfo").
-define(MARKETS, "markets").
-define(MARKETS_FOR_INSERT, "markets_for_insert").

-export([insert_game/4, 
         insert_market/3,
         delete_game/1,
         delete_market/1,
         pull_market/1,
         get_random_market/1,
         get_random_gameinfo/1,
         flush/0
        ]).

flush() ->
    {ok, RedisArgs} = application:get_env(sodomongo, redis_connection),
    {ok, C} = apply(eredis, start_link, RedisArgs),
    eredis:q(C, ["DEL", ?GAMEINFO]),
    eredis:q(C, ["DEL", ?MARKETS]),
    eredis:q(C, ["DEL", ?MARKETS_FOR_INSERT]).
    

insert_game(C, GameInfo, _MarketIds, Markets) ->
    [eredis:q(C, ["LPUSH", ?MARKETS_FOR_INSERT, term_to_binary(M)])  || M <- Markets],
    eredis:q(C, ["LPUSH", ?GAMEINFO, term_to_binary(GameInfo)]).

insert_market(C, MarketId, SelectionIds) ->
    eredis:q(C, ["LPUSH", ?MARKETS, term_to_binary({MarketId, SelectionIds})]).

delete_game(C) ->
    case eredis:q(C, ["RPOP", ?GAMEINFO]) of
        {ok, undefined} -> undefined;
        {ok, Bin} -> binary_to_term(Bin)
    end.

delete_market(C) ->
    eredis:q(C, ["RPOP", ?MARKETS]).
    
pull_market(C) ->
    {ok, Market} = eredis:q(C, ["RPOP", ?MARKETS_FOR_INSERT]),
    case Market of
        Market when is_binary(Market) -> binary_to_term(Market);
        Market -> Market
    end.

get_random_gameinfo(C) ->
    case  eredis:q(C, ["LLEN", ?GAMEINFO]) of
        {ok, <<"0">>} ->
            undefined;
        {ok, Size} ->
            S = list_to_integer(binary_to_list(Size)),
            Index = rand:uniform(S) - 1,
            {ok, GameInfo} = eredis:q(C, ["LINDEX", ?GAMEINFO, Index]),
            case GameInfo of
                undefined -> undefined;
                GameInfo -> binary_to_term(GameInfo)
            end
    end.


get_random_market(C) ->
    case  eredis:q(C, ["LLEN", ?MARKETS]) of
        {ok, <<"0">>} -> 
            undefined;
        {ok, Size} ->
            S = list_to_integer(binary_to_list(Size)),
            Index = rand:uniform(S) - 1,
            {ok, Market} = eredis:q(C, ["LINDEX", ?MARKETS, Index]),
            case Market of 
                undefined -> undefined;
                Market -> binary_to_term(Market)
            end
    end.
    
    

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


