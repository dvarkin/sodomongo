%%%-------------------------------------------------------------------
%%% @author serhiinechyporhuk
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 04. Oct 2016 15:20
%%%-------------------------------------------------------------------
-module(rethinkdb).
-author("serhiinechyporhuk").

%% API
-export([connect/1, r/2]).

connect(Args) ->
    RethinkConf = proplists:get_value(rethinkdb, Args),
    Host = proplists:get_value(host, RethinkConf),
    Port = proplists:get_value(port, RethinkConf),
    'Elixir.RethinkDB.Connection':start_link([{host, Host}, {port, Port}]).

apply_query(Fun, Args) ->
    apply('Elixir.RethinkDB.Query', Fun, Args).

r([[InitFun | InitArgs] | Ts], Conn) ->
    Query = lists:foldl(
        fun([Fun | Args], Acc) ->
            apply_query(Fun, [Acc | Args])
        end,
        apply_query(InitFun, InitArgs),
        Ts),
    'Elixir.RethinkDB':run(Query, Conn).