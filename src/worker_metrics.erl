-module(worker_metrics).
-author("eugeny").

-export([collect/0, notify/0, init/0, create_jobs/1]).

-define(INITIAL_STATUS_STAT, #{'WAIT_JOB' => 0, 'WAIT_CONNECTION' => 0, 'INPROGRESS' => 0, 'TOTAL' => 0}).

status_gauge_name(Status) ->
  list_to_binary(io_lib:format("workers.~s", [Status])).

job_gauge_name(Job) ->
  list_to_binary(io_lib:format("~s_metrics.workers.total", [Job])).

create_jobs(Jobs) ->
  [metrics:create(gauge, job_gauge_name(Job)) || Job <- Jobs].

collect() ->
  WorkersCount = maps:size(hugin:workers()),
  #{'WAIT_JOB' => 0, 'WAIT_CONNECTION' => 0, 'INPROGRESS' => WorkersCount, 'TOTAL' => WorkersCount}.


notify() ->
  ByStatus = collect(),
  [metrics:notify({status_gauge_name(Status), Count}) || {Status, Count} <- maps:to_list(ByStatus)].

init() ->
  [metrics:create(gauge, status_gauge_name(Status)) || Status <- maps:keys(?INITIAL_STATUS_STAT)].

