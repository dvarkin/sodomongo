%%% @author eugene
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 04. Oct 2016 4:52 PM
%%%-------------------------------------------------------------------
-module(redis).
-author("eugene").

-export([connect/1]).

connect(Args) ->
    RedisConf = proplists:get_value(redis, Args),
    Host = proplists:get_value(host, RedisConf),
    Port = proplists:get_value(port, RedisConf),
    eredis:start_link(Host, Port).
