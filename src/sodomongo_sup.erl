-module(sodomongo_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    Hugin = #{
        id => hugin,
        start => {hugin, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [hugin]
    },
    MetaStore = #{
        id => meta_storage,
        start => {meta_storage, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [meta_storage]
    },
    Metrics = #{
        id => metrics,
        start => {metrics, start_link, []},
        restart => permanent,
        shutdown => 5001,
        type => worker,
        modules => [metrics]
    },
    Procs = [Metrics, MetaStore, Hugin],
    {ok, {{one_for_one, 100, 500}, Procs}}.
