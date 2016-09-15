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
-export([rand_nth/1, parse_find_response/1, parse_command_response/1, timestamp_with_offset/2, rand_kv/1]).

rand_nth([H|[]]) -> H;

rand_nth([_|_] = List) ->
    Idx = rand:uniform(length(List)),
    lists:nth(Idx, List);

rand_nth(_) ->
    undefined.

rand_kv(undefined, undefined) -> undefined;
rand_kv(Key, Value) -> {Key, Value}.

rand_kv(Map) ->
    Key = rand_nth(maps:keys(Map)),
    Value = maps:get(Key, Map, undefined),
    rand_kv(Key, Value).



parse_find_response(Cursor) ->
    Data = mc_cursor:rest(Cursor),
    mc_cursor:close(Cursor),
    if
        Data == error -> #{ status => error, error_reason => "Unknown error" };
        true -> #{ status => success, doc_count => length(Data), result => Data }
    end.

parse_command_response(Response) ->
    case Response of
        {false, Error} ->
            #{ status => error, error_reason => Error };
        {true, #{ <<"result">> := Data}} ->
            #{ status => success, doc_count => length(Data), result => Data}
    end.

timestamp_with_offset(Timestamp, OffsetSet) ->
    SecInMega = 1000000,
    {Mega, Sec, Ms} = Timestamp,
    TotalSec = Mega * SecInMega + Sec,
    NewSec = TotalSec + OffsetSet,
    {NewSec div SecInMega, NewSec rem SecInMega, Ms}.