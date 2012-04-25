-define(HEADER_MAGIC, << 16#fe:8, 16#62:8, 16#69:8, 16#6e:8>>).
-record(reader_st, {location, fulldata, key, value, times}).
-record(binlog_st, {file, idx=0, position}).
-record(location_st, {file=null, filename, position}).
-record(keyoffset_st, {key, filename, pos}).

