-module(logstore).
-export([start/0]).
-include("common.hrl").

start() ->
    Filenames = lists:reverse(binlog:list_data_dir()),
    T = binlog:latest_filename(Filenames),
    spawn(fun() -> prepare(T) end).

prepare({FileIdx, FileName}) ->
    {ok, LogFile, Position} = binlog:open_for_write(FileName),
    Binlog = #binlog_st{file=LogFile, idx=FileIdx, position=Position},
    serve(binlog:try_rotate(Binlog)).

serve(Binlog) ->
    receive 
	{_, {set, Key, Value}} ->
	    {ok, Size} = binlog:write_event(Binlog#binlog_st.file, Key, Value),
	    NewPosition = Binlog#binlog_st.position + Size,
	    serve(binlog:try_rotate(Binlog#binlog_st{position=NewPosition}))
    end.
