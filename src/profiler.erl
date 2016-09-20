-module(profiler).

-export([prof/2]).

prof(_Metric, Fun) ->
    {_Time, Result} = timer:tc(Fun),
    %metrics:notify({Metric, Time}),
    Result.