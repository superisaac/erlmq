-module(binlog).
-export([open_for_read/1, open_for_write/1, read_event_list/2, 
	 read_event/1, write_event/3, make_filename/1,
	 each_event/3,
	 latest_filename/1, try_rotate/1, start_location/0,
	make_location/2, list_data_dir/0]).
-include("common.hrl").

make_fullname(Filename) ->
    DataDir = settings:get_key(data_dir),
    filename:join(DataDir, Filename).

make_location(Filename, Position) ->
    {ok, File} = open_for_read(Filename),
    {ok, NewPosition} = file:position(File, Position),
    #location_st{file=File, filename=Filename, position=NewPosition}.

list_data_dir() ->
    DataDir = settings:get_key(data_dir),
    lists:map(fun filename:basename/1,
	      filelib:wildcard("binlog.[0-9]*", DataDir)).
    
start_location() ->
    DataDir = settings:get_key(data_dir),
    case list_data_dir() of
	[Filename|_] ->
	    Location = make_location(Filename, 8),
	    {ok, Location};
	M ->
	    null
    end.

make_filename(T) ->
    Idx = string:right(integer_to_list(T), 6, $0),
    string:concat("binlog.", Idx).

latest_filename([]) ->
    T = 0,
    {T, binlog:make_filename(T)};
latest_filename([H|_]) ->
    I = get_fileno(H),
    {I, H}.

get_fileno(Filename) ->
    {match, [{Start, Length}]} = re:run(Filename, "[0-9]+$"),
    %T = string:strip(string:substr(Filename, Start + 1, Length), left, $0),
    T = string:substr(Filename, Start + 1, Length),
    list_to_integer(T).

open_for_read(Filename) ->
    FullFilename = make_fullname(Filename),
    {ok, File} = file:open(FullFilename, [read, raw, binary, read_ahead]),
    ok = read_head(File),
    {ok, File}.

open_for_write(Filename) ->
    FullFilename = make_fullname(Filename),
    {ok, File} = file:open(FullFilename, [append, raw, binary]),
    {ok, Position} = file:position(File, {eof, 0}),
    if 
	Position < 8 ->
	    write_head(File),
	    {ok, NewPosition} = file:position(File, {eof, 0}),
	    {ok, File, NewPosition};
	true ->
	    {ok, File, Position}
    end.

read_head(File) ->
    case file:read(File, 8) of
	{ok, Data} ->
	    <<16#fe:8, 16#62:8, 16#69:8, 16#6e:8, Version:8, Reserved:24>> = Data,
	    ok;
	M -> M
    end.	

write_head(File) ->
    Version = 1,
    Reserved = 0,
    Header = <<16#fe:8, 16#62:8, 16#69:8, 16#6e:8, Version:8, Reserved:24>>,
    file:write(File, Header).    
    
read_event_list(File, Func) ->
    case read_event(File) of
	{ok, Key, Data, Size} ->
	    Func(Key, Data),
	    read_event_list(File, Func);	
	M -> 
	    M
    end.

each_event(Location, 0, Func) ->
    {ok, Location};
each_event(Location, Times, Func) ->
    Obj = read_event_at_location(Location),
    case Obj of
	{ok, "binlog.rotate", Data, FullData, Location1} ->
	    file:close(Location#location_st.file),
	    NewFilename = binary_to_list(Data),
	    {ok, NewFile} = open_for_read(NewFilename),
	    {ok, NewPosition} = file:position(NewFile, {cur, 0}),
	    NewLocation = #location_st{file=NewFile,
				       filename=NewFilename,
				       position=NewPosition},
	    each_event(NewLocation, Times - 1, Func);
	{ok, Key, Data, FullData, NewLocation} ->
	    ReaderST = #reader_st{location=NewLocation,
				  fulldata=FullData,
				  key=Key,
				  value=Data,
				  times=Times},
	    case Func(ReaderST) of
		{break, Value} ->
		    {break, NewLocation, Value};
		_ ->
		    each_event(NewLocation, Times - 1, Func)
	    end;
	M -> {M, Location}
    end.
    
read_event(File) ->
    case file:read(File, 48) of
	{ok, Header} ->
	    <<Magic:32/little-unsigned,
	      Now:32/little-unsigned,
	      Xid:32/little-unsigned,
	      Size:32/little-unsigned,
	      ServerId:16/little-unsigned,
	      Type:8, Flags:8,
	      BKey:24/binary, 
	      Reserved:32/little-unsigned>> = Header,
	    Key = string:strip(binary_to_list(BKey), right, 0),
	    case file:read(File, Size) of
		{ok, Data} ->
		    {ok, Key, Data, Size + 48};
		M -> M
	    end;
	M -> M
    end.

read_event_at_location(Location) -> %File, CurrPos) ->
    case file:read(Location#location_st.file, 48) of
	{ok, Header} ->
	    <<Magic:32/little-unsigned,
	      Now:32/little-unsigned,
	      Xid:32/little-unsigned,
	      Size:32/little-unsigned,
	      ServerId:16/little-unsigned,
	      Type:8, Flags:8,
	      BKey:24/binary, 
	      Reserved:32/little-unsigned>> = Header,
	    Key = string:strip(binary_to_list(BKey), right, 0),
	    case file:read(Location#location_st.file, Size) of
		{ok, Data} ->
		    NewPos = Location#location_st.position + 48 + Size,
		    NewLocation = Location#location_st{position=NewPos},
		    {ok, Key, Data, <<Header/binary, Data/binary>>, NewLocation};
		M -> M
	    end;
	M -> M
    end.

write_event(File, Key, Data) ->
    {Mega, Sec, _} = now(),
    Now = Mega * 1000000 + Sec,
    Size = length(binary_to_list(Data)),
    ServerId = 1,
    Xid = 1,
    Type = 1,
    Flags = 0,
    BKey = list_to_binary(string:left(Key, 24, 0)),
    AllData = <<16#fe:8, 16#62:8, 16#69:8, 16#6e:8, 
	       Now:32/little-unsigned, Xid:32/little-unsigned,
	       Size:32/little-unsigned, ServerId:16/little-unsigned,
	       Type:8, Flags:8,  BKey:24/binary, 0:32/little-unsigned,
	       Data/binary
	      >>,
    ok = file:write(File, AllData),
    {ok, 48 + Size}.

try_rotate(Binlog) ->
    BinlogLimit = settings:get_key(binlog_size_limit),
    if
	Binlog#binlog_st.position > BinlogLimit ->
	    NewIdx = Binlog#binlog_st.idx + 1,
	    NewFilename = make_filename(NewIdx),
	    % Write rotate file to binlog.rotate
	    binlog:write_event(Binlog#binlog_st.file, "binlog.rotate", 
			       list_to_binary(NewFilename)),
	    file:close(Binlog#binlog_st.file),
	    io:format("Rotate to ~p~n", [NewFilename]),
	    {ok, NewLogFile, NewPosition} = binlog:open_for_write(NewFilename),
	    #binlog_st{file=NewLogFile, idx=NewIdx, position=NewPosition};
	true ->
	    Binlog
    end.

