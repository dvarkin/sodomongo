-module(init_test).

-include("generator.hrl").

-export([run/1]).

run(Connection) ->
    mc_worker_api:command(Connection, #{<<"dropDatabase">>, 1}),
    mc_worker_api:command(Connection, #{<<"enableSharding">>, ?DB}),

    mc_worker_api:command(Connection, #{<<"drop">> => ?GAMEINFO}),
    mc_worker_api:command(Connection, #{<<"drop">> => ?MARKETINFO}),

    mc_worker_api:command(Connection, #{<<"create">> => ?GAMEINFO}),
    mc_worker_api:command(Connection, #{<<"create">> => ?MARKETINFO}),

    mc_worker_api:ensure_index(Connection, ?MARKETINFO, #{<<"key">> => {?ID, <<"hashed">>}}),
    mc_worker_api:ensure_index(Connection, ?MARKETINFO, #{<<"key">> => {<<"Selections.ID">>, 1}}),
    mc_worker_api:ensure_index(Connection, ?GAMEINFO, #{<<"key">> => {?GAME_ID, <<"hashed">> }}),
    mc_worker_api:ensure_index(Connection, ?GAMEINFO, #{<<"key">> => {?BRANCH_ID, <<"hashed">> }}),

    mc_worker_api:command(Connection, #{
        <<"shardCollection">> => ?GAMEINFO,
        <<"key">> => #{?GAME_ID => <<"hashed">>}
    }),
    mc_worker_api:command(Connection, #{
        <<"shardCollection">> => ?MARKETINFO,
        <<"key">> => #{?ID => <<"hashed">>}
    }).



