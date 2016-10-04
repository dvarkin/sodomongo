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

make_args(_Dir) ->
    TunningArgs = " +P 2097152 +Q 1048576 -smp true +K true",
    MasterArgs = init:get_arguments(),
    DirDeps = string:join([" -" ++ atom_to_list(Key) ++" "++ Value || {Key, [Value]} <- MasterArgs, Key =:= pa orelse Key =:= setcookie orelse Key =:= config], ""),
    StartArgs = " -s sodomongo start_deps -s metrics start_link -s sodomongo init_metrics ",
    string:join([TunningArgs, DirDeps, StartArgs], " ").

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
    io:format("Start nodes with args: ~p", [Args]),
    {ok, Hosts} = application:get_env(sodomongo, hosts),
    Nodes = [slave:start(erlang:list_to_atom(Host), sodomongo_slave, Args) || Host <- Hosts],
    fallback(Nodes);

start(Nodes) ->
    error_logger:info_msg("Slave nodes: ~p", [Nodes]).

fallback([]) ->
    start();
fallback(Nodes) ->
    error_logger:info_msg("Slave nodes: ~p", [Nodes]).
