-module(logreader).
-export([start/0]).

start() ->
    spawn(fun() -> prepare() end).

prepare() ->
    {ok, ReadFile} = file:open('record.log', [read, raw, binary, read_ahead]),
    serve(ReadFile).

read_record(ReadFile, Location) ->
    case file:pread(ReadFile, Location, 22) of
	{ok, Head} ->
	    <<T:64/little, BKey:10/binary, VLen:32/little>> = Head,
	    Key = string:strip(binary_to_list(BKey)),
	    case file:pread(ReadFile, Location + 22, VLen) of
		{ok, Value} ->
		    {ok, Key, Value, T, Location + 22 + VLen};
		M -> M
	    end;
	M -> M
    end.

find_key(ReadFile, Key) ->
    find_key(ReadFile, Key, 0).
    
find_key(ReadFile, Key, Location) ->
    case read_record(ReadFile, Location) of
	{ok, Key, Value, T, NewLocation} ->
	    {ok, Key, Value, T, NewLocation};
	{ok, Key1, Value, _, NewLocation} ->
	    find_key(ReadFile, Key, NewLocation);
	M -> M
    end.

get_value(ReadFile, Key) ->	    
    case find_key(ReadFile, Key) of
	{ok, Key, Value, _, _} ->
	    Value;
	_ ->
	    <<>>
    end.    
    
serve(ReadFile) ->
    receive
	{Sender, {get, Key}} ->
	    BKey = list_to_binary(Key),
	    BValue = get_value(ReadFile, Key),
	    BLen = list_to_binary(integer_to_list(length(binary_to_list(BValue)))),
	    RetStr = <<"VALUE ", 
		       BKey/binary, " 0 ", 
		       BLen/binary,
		       "\r\n", BValue/binary, "\r\nEND\r\n">>,
	    Sender ! {response, RetStr},
	    serve(ReadFile)
    end.
