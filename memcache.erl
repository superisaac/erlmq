-module(memcache).
-export([start/2]).

start(Socket, LogStorePid) ->
    Worker = spawn(fun() -> receive_data(Socket, <<>>, LogStorePid) end),
    gen_tcp:controlling_process(Socket, Worker).

receive_data(Socket, Left, LogStorePid) ->
    receive
	{tcp, Socket, Data} ->
	    Buffer = <<Left/binary, Data/binary>>,
	    parse_packet(Socket, Buffer, LogStorePid);
	{tcp_closed, Socket} ->
	    gen_tcp:close(Socket),
	    ok;
	{response, Bin} ->
	    ok = gen_tcp:send(Socket, Bin),
	    receive_data(Socket, Left, LogStorePid)
    end.

return_get(Socket, Key, empty) ->
    BKey = list_to_binary(Key),
    RetStr = <<"VALUE ", 
	       BKey/binary, " 0 0", 
	       "\r\n\r\nEND\r\n">>,
    gen_tcp:send(Socket, RetStr);
return_get(Socket, Key, {value, Value}) ->
    BKey = list_to_binary(Key),
    BLen = list_to_binary(integer_to_list(length(binary_to_list(Value)))),
    RetStr = <<"VALUE ", 
	       BKey/binary, " 0 ", 
	       BLen/binary,
	       "\r\n", Value/binary, "\r\nEND\r\n">>,
    gen_tcp:send(Socket, RetStr).

parse_packet(Socket, <<>>, LogStorePid) ->
    receive_data(Socket, <<>>, LogStorePid);
parse_packet(Socket, Buffer, LogStorePid) ->
    case probe_packet(Buffer) of
	{packet, Obj, Rest} ->
	    case Obj of 
		{set, Key, Value} ->
		    gen_tcp:send(Socket, <<"STORED\r\n">>),
		    LogStorePid ! {self(), Obj},
		    QueuePid = myqueue:get_or_start(list_to_atom(Key)),
		    QueuePid ! {in, Value};		
		{get, Key} ->
		    QueuePid = myqueue:get_or_start(list_to_atom(Key)),
		    QueuePid ! {out, self()},
		    receive
			V -> return_get(Socket, Key, V)
		    end
	    end,
	    parse_packet(Socket, Rest, LogStorePid);
	uncomplete ->
	    receive_data(Socket, Buffer, LogStorePid);
	{error, Message} ->
	    io:format("Met error: ~p~n", [Message]),
	    {error, Message}
    end.

% Probe if a full Buffer exists
% if buffer is uncomplete then wait for new data
probe_packet(Buffer) ->
    case get_line(Buffer, <<>>) of
	{ok, Line, Rest}->
	    Tokens = string:tokens(Line, " "),
	    case Tokens of
		["set", Key, _, _, Size] ->
		    IntSize = list_to_integer(Size),
		    case get_body(IntSize, Rest) of
			{ok, Body, Rest1} ->
			    {packet, {set, Key, Body}, Rest1};
			uncomplete ->
			    uncomplete
		    end;
		["get", Key] ->
		    {packet, {get, Key}, Rest};
		_ ->
		    {error, "Protocol"}
	    end;
	uncomplete ->
	    uncomplete
    end.

get_body(IntSize, Data) ->
    case Data of 
	<<Body:IntSize/binary, "\r\n", Rest/binary >> ->
	    {ok, Body, Rest};
	_ ->
	    uncomplete
    end.

get_line(<<"\r\n", Rest/binary>>, Line) ->
    {ok, binary_to_list(Line), Rest};
get_line(<<>>, _) ->
    uncomplete;
get_line(<<C:8, Rest/binary>>, Line) ->
    get_line(Rest, list_to_binary(binary_to_list(Line) ++ [C])).
