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
    Hosts = proplists:get_value(host, AeroConf),
    Port = proplists:get_value(port, AeroConf),
    {ok, Conn} = aerospike:connect(hd(Hosts), Port),
    [aerospike:addhost(Conn, H, Port, 0) || H <- tl(Hosts)],
    {ok, Conn}.