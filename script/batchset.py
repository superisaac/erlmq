import sys
import memc
import time

mc = memc.Client(['127.0.0.1:12345'], debug=2)

if __name__ == '__main__':
    oldtime = time.time()
    #for v in sys.argv[1:]:
    for v in xrange(1, 5000):
        mc.set('abc', v)
    print time.time() - oldtime
