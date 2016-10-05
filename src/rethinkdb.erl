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
-export([connect/1, r/2, r/3]).

connect(Args) ->
    RethinkConf = proplists:get_value(rethinkdb, Args),
    Host = proplists:get_value(host, RethinkConf),
    Port = proplists:get_value(port, RethinkConf),
    'Elixir.RethinkDB.Connection':start_link([{host, Host}, {port, Port}]).

apply_query(Fun, Args) ->
    apply('Elixir.RethinkDB.Query', Fun, Args).

r(Ts, Conn) ->
    [[Fun | Args] | Rest] = Ts,
    r(Rest, apply_query(Fun, Args), Conn).

r([[Fun | Args] | Ts], Query, Conn) ->
    r(Ts, apply_query(Fun, [Query | Args]), Conn);

r([], Query, Conn) ->
    'Elixir.RethinkDB':run(Query, Conn).