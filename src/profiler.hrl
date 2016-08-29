-define(GPROF_TIME_METRIC(Body, Metric), 
        {Time, _} = timer:tc(fun() -> Body end),
        metrics:notify({Metric, Time})).


