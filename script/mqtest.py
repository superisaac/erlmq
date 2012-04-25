import sys
import memc
import time

mc = memc.Client(['127.0.0.1:12345'], debug=2)

if __name__ == '__main__':
    if len(sys.argv) <= 1:
        print >>sys.stderr, "Usage: %s [get <key>|set <key> <value>]"
        sys.exit(1)
    cmd = sys.argv[1]
    if cmd == 'get':
        print mc.get(sys.argv[2])
    elif cmd == 'set':
        mc.set(sys.argv[2], sys.argv[3])
    else:
        print >>sys.stderr, "Unknown command"
