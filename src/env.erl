-module(env).
-author("serhiinechyporhuk").

%% API
-export([get_env/1]).

get_env(Key) ->
    {ok, Val} = application:get_env(sodomongo, Key),
    Val.
