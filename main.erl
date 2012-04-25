-module(main).
-export([start/0, test/0]).
-define(TCP_OPTIONS, [binary, {packet, raw}, {reuseaddr, true}, {active, true}]).

start() ->
    settings:start(),
    spawn(fun () -> reader_start() end),
    spawn(fun () -> memcache_start() end).

reader_start() ->
    ReaderPort = settings:get_key(reader_port),
    {ok, Listen} = gen_tcp:listen(ReaderPort, ?TCP_OPTIONS),
    reader_loop(Listen).

reader_loop(Listen) ->
    {ok, Socket} = gen_tcp:accept(Listen),
    inet:setopts(Socket, ?TCP_OPTIONS),
    ok = reader:start(Socket),
    reader_loop(Listen).
    
    
memcache_start() ->
    LogStorePid = logstore:start(),
    MemcachedPort = settings:get_key(memcached_port),
    {ok, Listen} = gen_tcp:listen(MemcachedPort, ?TCP_OPTIONS),
    memcache_loop(Listen, LogStorePid).

memcache_loop(Listen, LogStorePid) ->
    {ok, Socket} = gen_tcp:accept(Listen),
    inet:setopts(Socket, ?TCP_OPTIONS),
    ok = memcache:start(Socket, LogStorePid),
    memcache_loop(Listen, LogStorePid).

test() ->
    {ok, Terms} = file:consult("config"),
    io:format("config terms ~p~n", [Terms]).
