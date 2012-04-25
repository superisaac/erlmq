-module(myqueue).
-export([get_or_start/1]).
-include("common.hrl").

% One process per key
get_or_start(Key) ->
    case whereis(Key) of
	undefined ->
	    spawn(fun () -> start(Key) end);
	Pid -> Pid
    end.

start(Key) ->
    register(Key, self()),
    keyoffset:new(),
    case keyoffset:retrieve(Key) of
	{value, Offset} ->
	    Location = binlog:make_location(Offset#keyoffset_st.filename,
					    Offset#keyoffset_st.pos),
	    serve(Key, Location);
	start ->
	    serve(Key, start)
    end.

find_next_key(Sender, Key, start) ->
    {ok, Location} = binlog:start_location(),
    find_next_key(Sender, Key, Location);
find_next_key(Sender, Key, Location) ->
    {ok, Position} = file:position(Location#location_st.file,
				   Location#location_st.position),
    Ret = binlog:each_event(Location, 100,
			    fun (ReaderInfo) ->
				    SKey = atom_to_list(Key),
				    case ReaderInfo#reader_st.key of
					SKey ->
					    {break, 
					     ReaderInfo#reader_st.value};
					M -> 
					    M
				    end
			    end),
    case Ret of
	{break, Loc, Value} ->
	    Sender ! {value, Value},
	    {ok, Loc};	
	{M, Loc} ->
	    Pos = file:position(Loc#location_st.file, {cur, 0}),
	    Sender ! empty,
	    {ok, Loc};
	M ->
	    Sender ! empty,
	    {error, Location, M}
    end.

serve(Key, Location) ->
    receive
	{in, Value} ->
	    serve(Key, Location);
	{out, Sender} ->
	    case find_next_key(Sender, Key, Location) of
		{ok, NewLoc} ->
		    keyoffset:update(Key, NewLoc#location_st.filename,
				     NewLoc#location_st.position),
		    serve(Key, NewLoc);
		{error, NewLoc, M} ->
		    keyoffset:update(Key, NewLoc#location_st.filename,
				     NewLoc#location_st.position),
		    serve(Key, NewLoc)
	    end
    end.

