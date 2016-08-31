%%%-------------------------------------------------------------------
%%% @author Dmitry Omelechko <dvarkin@gmail.com>
%%% @copyright (C) 2016, Dmitry Omelechko
%%% @doc
%%%
%%% @end
%%% Created : 30 Aug 2016 by Dmitry Omelechko <dvarkin@gmail.com>
%%%-------------------------------------------------------------------
-module(cloud).

-export([start/0]).

make_args(Dir) ->
    Deps = [
        "/ebin",
        "/deps/bson/ebin",
        "/deps/mongodb/ebin",
        "/deps/pbkdf2/ebin",
        "/deps/poolboy/ebin",
        "/deps/bear/ebin",
        "/deps/folsom/ebin",
        "/deps/folsomite/ebin",
        "/deps/zeta/ebin",
        "/deps/protobuffs/ebin",
        "/deps/sync/ebin"
    ],
    DirDeps = string:join([" -pa " ++ Dir ++ D || D <- Deps], ""),
    StartArgs = " -config " ++ Dir ++ "/rel/sys -setcookie abc -s sodomongo start_deps ",
    string:join([DirDeps, StartArgs], " ").

start() ->
    %error_logger:info_msg("Start slaves with ~p", [Args]),
    error_logger:info_msg("Fallback to  net_adm:world() ...", []),
    {ok, Hosts} = application:get_env(sodomongo, hosts),
    Nodes = net_adm:world_list(lists:map(fun erlang:list_to_atom/1, Hosts)),
    start(Nodes).

start([]) ->
    error_logger:info_msg("Start slaves ...", []),
    {ok, Dir} = file:get_cwd(),
    Args = make_args(Dir),
    Nodes = pool:start(sodomongo_slave, Args),
    fallback(Nodes);
start(Nodes) ->
    error_logger:info_msg("Slave nodes: ", [Nodes]).

fallback([]) ->
    start();
fallback(Nodes) ->
    error_logger:info_msg("Slave nodes: ", [Nodes]).
