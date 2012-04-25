-module(settings).
-export([start/0, get_key/1]).
-include("common.hrl").

start() ->
    {ok, Terms} = file:consult("config"),
    register(?MODULE, spawn(fun () ->
				     put_keys(Terms),
				     serve() end)).

put_keys([]) ->
    ok;
put_keys([H|T]) ->
    {K, V} = H,
    put(K, V),
    put_keys(T).

serve() ->
    receive
	{Sender, get, K} ->
	    case get(K) of
		undeinfed ->
		    Sender ! {return, undefined};
		V ->
		    Sender ! {return, {value, V}}
	    end
    end,
    serve().

get_key(K) ->
    ?MODULE !  {self(), get, K},
    receive
	{return, {value, V}} ->
	    V;
	{return, undefined} ->
	    undefined
    end.

	    


    

    


