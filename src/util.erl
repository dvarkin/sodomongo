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
-export([rand_nth/1]).

rand_nth([_|_] = List) ->
    Idx = rand:uniform(length(List)),
    lists:nth(Idx, List);
rand_nth(_) ->
    undefined.

