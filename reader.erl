-module(reader).
-export([start/1]).
-include("common.hrl").

start(Socket) ->
    Worker = spawn(fun() -> receive_data(Socket, <<>>) end),
    gen_tcp:controlling_process(Socket, Worker).

receive_data(Socket, Left) ->
    receive
	{tcp, Socket, Data} ->
	    Buffer = <<Left/binary, Data/binary>>,
	    parse_request(Socket, Buffer)
    end.

parse_request(Socket, <<>>) ->
    receive_data(Socket, <<>>);
parse_request(Socket, Buffer) ->
    case Buffer of
	<<Magic:32/little-unsigned,
	  Version:8,
	  Cmd:8,
	  ServerId:16/little-unsigned,
	  ServerName:16/binary,
	  BodySize:32/little-unsigned,
	  LogName:16/binary,
	  LogPos:32/little-unsigned>> ->
	    send_stream(Socket, LogName, LogPos);
	_ ->
	    receive_data(Socket, Buffer)
    end.

send_stream(Socket, LogName, LogPos) ->
    {ok, Location} = binlog:make_location(LogName, LogPos),
    {ok, NewPos} = file:position(Location#location_st.file,
				 Location#location_st.position),
    Ret = binlog:each_event(Location, 10,
			    fun (ReaderInfo) ->
				    Loc = ReaderInfo#reader_st.location,
				    Filename = string:left(Loc#location_st.filename, 16, 0),
				    Fulldata = ReaderInfo#reader_st.fulldata,
				    Pos = Loc#location_st.position,
				    Bin = <<Fulldata/binary,
					    Filename/binary,
					    Pos:32/little-unsigned>>,
				    gen_tcp:send(Socket, Bin)
			    end),
    {_, Loc} = Ret,
    file:close(Loc#location_st.file),
    gen_tcp:close(Socket).



	    
