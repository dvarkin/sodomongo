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
-export([connect/1, r/2, r/3, next/1]).

connect(Args) ->
    RethinkConf = proplists:get_value(rethinkdb, Args),
    Host = proplists:get_value(host, RethinkConf),
    Port = proplists:get_value(port, RethinkConf),
    'Elixir.RethinkDB.Connection':start_link([{host, Host}, {port, Port}]).

apply_query(Fun, Args) ->
    apply('Elixir.RethinkDB.Query', Fun, Args).


%% @doc
%% Usage:
%%  Response = rethinkdb:r([
%%      [table, ?GAMEINFO],
%%      [changes]
%%    ],
%%    Conn),
%% @end
r(Queries, Conn) ->
    [[Fun | Args] | Rest] = Queries,
    r(Rest, apply_query(Fun, Args), Conn).

r([[Fun | Args] | Queries], ResultQuery, Conn) ->
    r(Queries, apply_query(Fun, [ResultQuery | Args]), Conn);

r([], ResultQuery, Conn) ->
    'Elixir.RethinkDB':run(ResultQuery, Conn).

next(Result) ->
    'Elixir.RethinkDB':next(Result).
