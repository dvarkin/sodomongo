-module(init_test).

-include("generator.hrl").

-export([run/1]).

run(AdminConnection) ->
    {true, _} = mc_worker_api:command(AdminConnection, #{<<"dropDatabase">> => 1}),
    {true, _} = mc_worker_api:command(AdminConnection, #{<<"enableSharding">> => ?DB}),
    {true, _} = mc_worker_api:command(AdminConnection, #{<<"drop">> => ?GAMEINFO}),
    {true, _} = mc_worker_api:command(AdminConnection, #{<<"drop">> => ?MARKETINFO}),
    {true, _} = mc_worker_api:command(AdminConnection, #{<<"create">> => ?GAMEINFO}),
    {true, _} = mc_worker_api:command(AdminConnection, #{<<"create">> => ?MARKETINFO}),

    mc_worker_api:ensure_index(AdminConnection, ?MARKETINFO, #{<<"key">> => {?ID, <<"hashed">>}}),
    mc_worker_api:ensure_index(AdminConnection, ?MARKETINFO, #{<<"key">> => {<<"Selections.ID">>, 1}}),
    mc_worker_api:ensure_index(AdminConnection, ?GAMEINFO, #{<<"key">> => {?GAME_ID, <<"hashed">> }}),
    mc_worker_api:ensure_index(AdminConnection, ?GAMEINFO, #{<<"key">> => {?BRANCH_ID, <<"hashed">> }}),

    {true, _} = mc_worker_api:command(AdminConnection, #{
        <<"shardCollection">> => ?GAMEINFO,
        <<"key">> => #{?GAME_ID => <<"hashed">>}
    }),
    {true, _} = mc_worker_api:command(AdminConnection, #{
        <<"shardCollection">> => ?MARKETINFO,
        <<"key">> => #{?ID => <<"hashed">>}
    }).



