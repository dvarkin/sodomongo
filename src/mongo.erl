%%%-------------------------------------------------------------------
%%% @author serhiinechyporhuk
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 08. Sep 2016 14:15
%%%-------------------------------------------------------------------
-module(mongo).
-author("serhiinechyporhuk").

%% API
-export([conn_args/0, connect_to_master/1, connect_to_secondary/1]).


is_master(Conn) ->
    {_, #{<<"ismaster">> := IsMaster}} = mc_worker_api:command(Conn, {<<"isMaster">>, 1}),
    IsMaster.

connect(ConnectionArgs, Host, Database) ->
    C1 = lists:keyreplace(host, 1, ConnectionArgs, {host, Host}),
    C2 = lists:keyreplace(database, 1, C1, {databse, Database}),
    mc_worker_api:connect(C2).

group_nodes(ConnectionArgs) ->
    Hosts = proplists:get_value(host, ConnectionArgs),
    group_nodes(Hosts, ConnectionArgs, []).

group_nodes([Host | Hosts], ConnectionArgs, Secondaries) ->
    {ok, Conn} = connect(ConnectionArgs, Host, <<"admin">>),
    case is_master(Conn) of
        true -> group_nodes(Hosts, ConnectionArgs, Host, Secondaries);
        false -> group_nodes(Hosts, ConnectionArgs, [Host | Secondaries])
    end;

group_nodes([], _, _) ->
    error_logger:error_msg("Can't find master node"),
    undefined.

group_nodes(Hosts, ConnectionArgs, Master, Secondaries) ->
    #{
        master => lists:keyreplace(host, 1, ConnectionArgs, {host, Master}),
        secondaries => [lists:keyreplace(host, 1, ConnectionArgs, {host, Sec}) || Sec <- lists:concat([Hosts, Secondaries])]
    }.

conn_args() ->
    {ok, RawConnArgs} = application:get_env(sodomongo, mongo_connection),
    group_nodes(RawConnArgs).

connect_to_master(#{ master := MasterConnArgs }) ->
    mc_worker_api:connect(MasterConnArgs).

connect_to_secondary(#{ secondaries := SecondariesConnArgs }) ->
    HostConfig = util:rand_nth(SecondariesConnArgs), 
    ConnArgs = [{r_mode, slave_ok} | HostConfig],
    mc_worker_api:connect(ConnArgs).
