%%%-------------------------------------------------------------------
%%% @author serhiinechyporhuk
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 26. Aug 2016 13:54
%%%-------------------------------------------------------------------
-module(read_live_gameinfo_by_branch_task).
-author("serhiinechyporhuk").

%% API
-export([run/1]).

-define(TASK_SLEEP, 1000).
-define(RATE, atom_to_binary(?MODULE, utf8)).

create_indexes(Connection) ->
    mc_worker_api:ensure_index(Connection, <<"gameinfo">>, #{<<"key">> => {<<"BranchID">>, <<"hashed">>}}).

job(Connection) ->

    {_Status, _Res} = mc_worker_api:command(Connection, #{
        <<"aggregate">> => <<"gameinfo">>,
        <<"pipeline">> => [
            #{
                <<"$group">> => #{
                    <<"_id">> => #{<<"BranchID">> => <<"$BranchID">>},
                    <<"gameinfos">> => #{
                        <<"$push">> => #{
                            <<"gameinfo">> => <<"$$CURRENT">>
                        }
                    }
                }
            }
        ]
    }),

    metrics:notify({?RATE, 1}),
    timer:sleep(?TASK_SLEEP),

    job(Connection).

run(Connection) ->
    create_indexes(Connection),
    metrics:create(meter, ?RATE),
    job(Connection).