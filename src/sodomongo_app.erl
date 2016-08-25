-module(sodomongo_app).
-behaviour(application).

-export([start/2]).
-export([stop/1]).

start(_Type, _Args) ->
    
%% TODO >>> FIX THIS SHIT

    _Args =
        " -pa /vagrant/sodomongo/deps/bson/ebin"
        " -pa /vagrant/sodomongo/deps/mongodb/ebin"
        " -pa /vagrant/sodomongo/deps/pbkdf2/ebin/"
        " -pa /vagrant/sodomongo/deps/poolboy/ebin/"
        " -pa /vagrant/sodomongo/ebin/"
        " -pa /vagrant/sodomongo/deps/bear/ebin/"
        " -pa /vagrant/sodomongo/deps/folsom/ebin/"
        " -pa /vagrant/sodomongo/deps/folsomite/ebin/"
        " -pa /vagrant/sodomongo/deps/zeta/ebin/"
        " -pa /vagrant/sodomongo/deps/protobuffs/ebin/"
        " -s sodomongo start_deps",

%%% <<< TODO
    
    Nodes = pool:start(mongo_tester),
    
    error_logger:info_msg("CLUSTER OF: ~p~n", [Nodes]),
    sodomongo_sup:start_link().

stop(_State) ->
	ok.
