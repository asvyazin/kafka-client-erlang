-module(message_set).
-compile({parse_transform, do}).

-export([put/2, get_message/1, get/1]).

-include("api.hrl").

-define(SIZEOF_CRC, 4).

get_message_size(#message{ key = Key, value = Value }) ->
    4 + 1 + 1 + 4 + byte_size(Key) + 4 + byte_size(Value).

get_size(Msg) ->
    8 + 4 + get_message_size(Msg).

get_message(Get) ->
    do([Get || Offset <- get:int64_big(Get),
	       MessageSize <- get:int32_big(Get),
	       Crc32 <- get:int32_big(Get),
	       MagicByte <- get:int8(Get),
	       Attributes <- get:int8(Get),
	       Key <- get:bytes(Get),
	       Value <- get:bytes(Get),
	       return(#message{ offset = Offset, key = Key, value = Value })]).

get(Get) ->
    do([Get || MessageSetSize <- get:int32_big(Get),
	       get(Get, MessageSetSize, [])]).

get(Get, 0, Res) ->
    do([Get || return(lists:reverse(Res))]);
get(Get, MessageSetSize, Res) ->
    do([Get || Message <- get_message(Get),
	       get(Get, MessageSetSize - get_size(Message), [Message | Res])]).

put(Put, MessageSet) ->
    monad:sequence(Put, [put_message(Put, X) || X <- MessageSet]).

put_message(Put, #message{ key = Key, value = Value }) ->
    do([Put || put:int64_big(Put, -1),
	       MessageBytes = put:run(Put, do([Put || put:int8(Put, ?MESSAGE_MAGIC_BYTE),
						      put:int8(Put, 0),
						      put:bytes(Put, Key),
						      put:bytes(Put, Value)])),
	       put:int32_big(Put, byte_size(MessageBytes) + ?SIZEOF_CRC),
	       put:int32_big(Put, erlang:crc32(MessageBytes)),
	       put:bytes_without_prefix(Put, MessageBytes)]).
