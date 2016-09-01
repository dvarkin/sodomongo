-module(generator).
-include("generator.hrl").
-export([new_market_info/0, 
         new_market_info/2,
         new_game_info/0,
         new_game_info/2, 
         new_game_with_markets/0,
         new_selections/2]).


%% TOOLS

-export([rand_nth/1, new_odd/0]).


-define(DONT_GIVE_A_FUCK, 42).

%%% Limits %%%
-define(MIN_ID, 100000).
-define(MAX_ID, 100000000).
-define(MIN_GENERATION, 1).
-define(MAX_GENERATION, 1000).
-define(MIN_BRANCH, 1).
-define(MAX_BRANCH, 30).
-define(MIN_LEAGUE_ID, 1).
-define(MAX_LEAGUE_ID, 1000).
-define(MIN_GROUP_ID, 1).
-define(MAX_GROUP_ID, 100000).
-define(MIN_REGION_ID, 1).
-define(MAX_REGION_ID, 1000).
-define(MIN_GAME_ID, 100000).
-define(MAX_GAME_ID, 10000000).
-define(MIN_TOTAL_BETS, 1).
-define(MAX_TOTAL_BETS, 1000).
-define(MIN_TOTAL_DEPOSIT_GBP, 10).
-define(MAX_TOTAL_DEPOSIT_GBP, 100000).

%%% Helpers %%%
rand_int(From, To) ->
  rand:uniform(To - From) + From.

rand_bool() ->
  maps:get(rand:uniform(2) - 1, #{0 => false, 1 => true}).

rand_nth(List) ->
  Index = rand:uniform(length(List)),
  lists:nth(Index, List).

rand_kv(Map) ->
  Key = rand_nth(maps:keys(Map)),
  Value = maps:get(Key, Map),
  {Key, Value}.


rand_names() ->
  #{
    <<"0">> => <<"foo">>,
    <<"1">> => <<"bar">>,
    <<"2">> => <<"baz">>,
    <<"3">> => <<"quux">>,
    <<"4">> => <<"quuux">>
  }.

new_bet_statistics() ->
  #{
    ?TOTAL_BETS => rand_int(?MIN_TOTAL_BETS, ?MAX_TOTAL_BETS),
    ?TOTAL_DEPOSIT_GBP => rand_int(?MIN_TOTAL_DEPOSIT_GBP, ?MAX_TOTAL_DEPOSIT_GBP)
  }.

new_market_id() ->
  Types = ["QA", "ML", "HC", "OU"],
  Type = rand_nth(Types),
  ID = rand_int(?MIN_ID, ?MAX_ID),
  list_to_binary(Type ++ integer_to_list(ID)).

new_side() ->
  rand_int(1, 2).

new_odd(negative) ->
  +5 * rand:uniform(1000);
new_odd(positive) ->
  -5 * rand:uniform(4000);
new_odd(zero) ->
  0.

new_odd() ->
  new_odd(rand_nth([negative, negative, negative, positive, positive, positive, zero])).

new_game_status() ->
  GameStatuses = #{
    'STATUS_NOT_STARTED' => 0,
    'STATUS_FIRSTHALF' => 1,
    'STATUS_SECONDHALF' => 2,
    'STATUS_HALFTIME' => 4,
    'STATUS_FINISHED' => 6,
    'STATUS_OVERTIME' => 9
  },
  {_, Status} = rand_kv(GameStatuses),
  Status.

new_selection_info(Extras) ->
  BaseSelection = #{
    ?ID => rand_int(?MIN_ID, ?MAX_ID),
    ?GENERATION => rand_int(?MIN_GENERATION, ?MAX_GENERATION),
    ?SIDE => new_side(),
    ?SELECTION_TYPE => rand_nth([1, 42, 88, 14, 25, 60]), %% I don't know
    ?GROUP_ID => rand_int(?MIN_GROUP_ID, ?MAX_GROUP_ID),
    ?NAME => rand_names(),
    ?TITLE => rand_names(),
    ?ODDS => new_odd(),
    ?POINTS => ?DONT_GIVE_A_FUCK %% I don't know
  },

  Params = #{
    ?QAPARAM1 => ?DONT_GIVE_A_FUCK, %% opt
    ?QAPARAM2 => ?DONT_GIVE_A_FUCK %% opt
  },

  Selection = case rand_bool() of
    true ->
      maps:merge(BaseSelection, Params);
    false ->
      BaseSelection
  end,

  maps:merge(Selection, Extras).

new_selections(From, To) ->
    [new_selection_info(#{?GROUP_ID => 10}) || _ <- lists:seq(1, rand_int(From, To))].

new_market_info() ->
  #{
    ?ID => new_market_id(),
    ?GENERATION => rand_int(?MIN_GENERATION, ?MAX_GENERATION),
    ?GAME_ID => rand_int(?MIN_GAME_ID, ?MAX_GAME_ID), %OR >
    ?LEAGUE_ID => rand_int(?MIN_LEAGUE_ID, ?MAX_LEAGUE_ID), %OR ^
    ?NAME => rand_names(),
    ?TITLE => rand_names(),
    ?TEAM_MAPPING => ?DONT_GIVE_A_FUCK,
    ?START_DATE => os:timestamp(),
    ?TEAM_SWAP => rand_bool(),
    ?MARKET_BETS => new_bet_statistics(),
    ?SELECTIONS => new_selections(10,20) 
  }.

new_market_info(leagueId, LeagueId) ->
  maps:put(?LEAGUE_ID, LeagueId, maps:remove(?GAME_ID, new_market_info()));
new_market_info(gameId, GameId) ->
  maps:put(?GAME_ID, GameId, maps:remove(?LEAGUE_ID, new_market_info())).

new_game_info() ->
  #{
    ?ID => rand_int(?MIN_ID, ?MAX_ID),
    ?GENERATION => rand_int(?MIN_GENERATION, ?MAX_GENERATION),
    ?BRANCH_ID => rand_int(?MIN_BRANCH, ?MAX_BRANCH),
    ?LEAGUE_ID => rand_int(?MIN_LEAGUE_ID, ?MAX_LEAGUE_ID),
    ?REGION_ID => rand_int(?MIN_REGION_ID, ?MAX_REGION_ID),
    ?START_DATE => os:timestamp(),
    ?IS_ACTIVE => rand_bool(),
    ?IS_LIVE => rand_bool(),
    ?GAME_BETS => new_bet_statistics(),
    ?STATUS => new_game_status(),
    ?HOME_TEAM_NAME => rand_names(),
    ?AWAY_TEAM_NAME => rand_names()
  }.

new_game_info(GameId, LeagueId) ->
  maps:merge(new_game_info(), #{?ID => GameId, ?LEAGUE_ID => LeagueId}).

new_game_with_markets() ->
  GameId = rand_int(?MIN_ID, ?MAX_ID),
  LeagueId = rand_int(?MIN_LEAGUE_ID, ?MAX_LEAGUE_ID),

  GameInfo = new_game_info(GameId, LeagueId),
  MarketInfos = [new_market_info(gameId, GameId) || _ <- lists:seq(1, 10)],
  {GameInfo, MarketInfos}.
