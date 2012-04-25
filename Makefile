.SUFFIXES: .erl .beam
.erl.beam:
	erlc -W $<

MODS=main memcache logstore binlog reader myqueue keyoffset settings

all: compile

run: all
	erl -noshell -s main start

test: all
	erl -noshell -s main test

compile: ${MODS:%=%.beam}

clean:
	rm -Rf ${MODS:%=%.beam} *.dump