%%%-------------------------------------------------------------------
%%% @author eugene
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 19. Oct 2016 2:27 PM
%%%-------------------------------------------------------------------
-module(shit_generator).
-author("eugene").

%% API
-export([gen/0]).

gen() ->
    [rand:uniform(9) || _ <- lists:seq(1, 1024)].


