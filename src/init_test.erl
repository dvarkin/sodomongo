-module(init_test).

-include("generator.hrl").

-export([run/1]).

run(Connection) ->
    mc_worker_api:command(Connection, #{<<"drop">> => ?GAMEINFO}),
    mc_worker_api:command(Connection, #{<<"drop">> => ?MARKETINFO}),
    mc_worker_api:ensure_index(Connection, ?MARKETINFO, #{<<"key">> => {?ID, <<"hashed">>}}),
    mc_worker_api:ensure_index(Connection, ?MARKETINFO, #{<<"key">> => {<<"Selections.ID">>,1}}).
