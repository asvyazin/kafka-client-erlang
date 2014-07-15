-module(message_set).
-compile({parse_transform, do}).

-export([put/2]).

-include("api.hrl").

-define(SIZEOF_CRC, 4).

put(Put, MessageSet) ->
    monad:sequence(Put, [put_message(Put, X) || X <- MessageSet]).

put_message(Put, #message{ key = Key, value = Value }) ->
    do([Put || put:int64_big(Put, 0),
	       MessageBytes = put:run(Put, do([Put || put:int8(Put, ?MESSAGE_MAGIC_BYTE),
						      put:int8(Put, 0),
						      put:bytes(Put, Key),
						      put:bytes(Put, Value)])),
	       put:int32_big(Put, byte_size(MessageBytes) + ?SIZEOF_CRC),
	       put:int32_big(Put, erlang:crc32(MessageBytes)),
	       put:bytes_without_prefix(Put, MessageBytes)]).
