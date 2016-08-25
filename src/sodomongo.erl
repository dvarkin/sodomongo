-module(sodomongo).

-export([start/0, start_deps/0, start_test/3]).

start() ->
    start_deps(),
    ok = application:start(sodomongo),
    net_adm:world().

start_test(Workers, Rate, Mins) ->
    Pids = [Pid || {ok, Pid} <- [muter_sup:start_kinder() || _ <- lists:seq(1, Workers)]],
    [kinder:job(Pid, {start, Rate, Mins}) || Pid <- Pids].

start_deps() ->
    ok = application:ensure_started(crypto),
    ok = application:ensure_started(poolboy),
    ok = application:ensure_started(bson),
    ok = application:ensure_started(mongodb),
    ok = application:ensure_started(bear),
    ok = application:ensure_started(folsom),
    ok = application:ensure_started(protobuffs),
    ok = application:ensure_started(zeta),
    ok = application:ensure_started(folsomite),
    ok = application:ensure_started(compiler),
    ok = application:ensure_started(syntax_tools).

