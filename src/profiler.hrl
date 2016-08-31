-define(GPROF_TIME_METRIC(Body, Metric), 
        {Time, Result} = timer:tc(fun() -> Body end),
        metrics:notify({Metric, Time}),
        Result).


