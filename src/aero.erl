%%%-------------------------------------------------------------------
%%% @author eugene
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 19. Oct 2016 2:37 PM
%%%-------------------------------------------------------------------
-module(aero).
-author("eugene").

%% API
-export([connect/1]).

connect(Args) ->
    AeroConf = proplists:get_value(aerospike, Args),
    Host = proplists:get_value(host, AeroConf),
    Port = proplists:get_value(port, AeroConf),
    aerospike:connect(Host, Port).