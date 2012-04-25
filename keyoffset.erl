-module(keyoffset).
-export([new/0, retrieve/1, update/3, test/0]).
-define(DETSKEY, koffset).
-include("common.hrl").

new() ->
    DataDir = settings:get_key(data_dir),
    KOfFile = filename:join(DataDir, "keyoffset.db"),
    dets:open_file(?DETSKEY, [{type, set}, 
			      {file, KOfFile},
			      {keypos, #keyoffset_st.key}]).

retrieve(Key) ->
    Obj = dets:lookup(?DETSKEY, Key),
    case Obj of
	[H|_] ->
	    {value, H};
	[] ->
	    start;
	{error, Reason} ->
	    {error, Reason}
    end.

update(Key, Filename, Pos) ->
    Offset = #keyoffset_st{key=Key, filename=Filename, pos=Pos},
    dets:insert(?DETSKEY, Offset).

test() ->
    new(),
    update(nnn, "Hel", 6),
    Ret = retrieve(nnn),
    io:format("Ret is ~p~n", [Ret]).
