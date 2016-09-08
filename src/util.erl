%%%-------------------------------------------------------------------
%%% @author serhiinechyporhuk
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 29. Aug 2016 13:30
%%%-------------------------------------------------------------------
-module(util).
-author("serhiinechyporhuk").

%% API
-export([rand_nth/1, parse_find_response/1]).

rand_nth([H|[]]) -> H;

rand_nth([_|_] = List) ->
    Idx = rand:uniform(length(List)),
    lists:nth(Idx, List);

rand_nth(_) ->
    undefined.

parse_find_response(Cursor) ->
    Data = mc_cursor:rest(Cursor),
    mc_cursor:close(Cursor),
    if
        Data == error -> #{ status => error, error_reason => "Unknown error" };
        true -> #{ status => success, doc_count => length(Data), result => Data }
    end.