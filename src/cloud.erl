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
%    TunningArgs = " +P 2097152 +Q 1048576 -env ERTS_MAX_PORTS 1048576 -env ERL_FULLSWEEP_AFTER 1000 -smp auto +K true +sfwi 500 +A 100 +zdbbl 102400",
    TunningArgs = " +P 2097152 +Q 1048576 -smp true +K true",
    ElixirEbin = os:getenv("ELIXIR_EBIN", ""),

    Deps = [
    "/_build/dev/lib/bson/ebin",
    "/_build/dev/lib/mongodb/ebin",
    "/_build/dev/lib/pbkdf2/ebin/",
    "/_build/dev/lib/poolboy/ebin/",
    "/_build/dev/lib/bear/ebin/",
    "/_build/dev/lib/folsom/ebin/",
    "/_build/dev/lib/folsomite/ebin/",
    "/_build/dev/lib/protobuffs/ebin/",
    "/_build/dev/lib/sync/ebin/",
    "/_build/dev/lib/sodomongo/ebin",
    "/_build/dev/lib/rethinkdb/ebin",
    "/_build/dev/lib/connection/ebin",
    "/_build/dev/lib/eredis/ebin",
    "/_build/dev/lib/poison/ebin"
    ],
    DirDeps = string:join([" -pa " ++ Dir ++ D || D <- Deps], ""),
    StartArgs = "-pa " ++ ElixirEbin ++ " -config "  ++ Dir ++ "/rel/sys -setcookie abc -s sodomongo start_deps -s metrics start_link -s sodomongo init_metrics ",
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
    {ok, Hosts} = application:get_env(sodomongo, hosts),
    Nodes = [slave:start(erlang:list_to_atom(Host), sodomongo_slave, Args) || Host <- Hosts],
    fallback(Nodes);

start(Nodes) ->
    error_logger:info_msg("Slave nodes: ~p", [Nodes]).

fallback([]) ->
    start();
fallback(Nodes) ->
    error_logger:info_msg("Slave nodes: ~p", [Nodes]).
