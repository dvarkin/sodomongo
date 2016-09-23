-module(profiler).

-export([prof/2]).

prof(Metric, Fun) ->
    {Time, Result} = timer:tc(Fun),
    metrics:notify({Metric, Time}),
    Result.