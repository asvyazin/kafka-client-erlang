-module(get).
-compile({parse_transform, do}).
-export([new/0, int8/1, int16_big/1, int32_big/1, int64_big/1, string/1, bytes/1, array/2, error_code/1]).

new() ->
    state_t:new(identity_m).

int8(StateT) ->
    StateT:modify_and_return(fun(<<Value:8/integer, Rest/binary>>) -> {Value, Rest} end).

int16_big(StateT) ->
    StateT:modify_and_return(fun(<<Value:16/integer-big, Rest/binary>>) -> {Value, Rest} end).

int32_big(StateT) ->
    StateT:modify_and_return(fun(<<Value:32/integer-big, Rest/binary>>) -> {Value, Rest} end).

int64_big(StateT) ->
    StateT:modify_and_return(fun(<<Value:64/integer-big, Rest/binary>>) -> {Value, Rest} end).

string(StateT) ->
    StateT:modify_and_return(fun(<<Length:16/integer-big, Data:Length/binary, Rest/binary>>) -> {Data, Rest} end).

bytes(StateT) ->
    StateT:modify_and_return(fun(<<Length:32/integer-big, Data:Length/binary, Rest/binary>>) -> {Data, Rest} end).

array(StateT, G) ->
    do([StateT || Length <- int32_big(StateT),
		  monad:sequence(StateT, lists:duplicate(Length, G))]).

error_code(StateT) ->
    do([StateT || Code <- int16_big(StateT),
		  return(convert_error_code(Code))]).

convert_error_code(0) ->
    ok;
convert_error_code(-1) ->
    unknown_error;
convert_error_code(1) ->
    offset_out_of_range;
convert_error_code(2) ->
    invalid_message;
convert_error_code(3) ->
    unknown_topic_or_partition;
convert_error_code(4) ->
    invalid_message_size;
convert_error_code(5) ->
    leader_not_available;
convert_error_code(6) ->
    not_leader_for_partition;
convert_error_code(7) ->
    request_timed_out;
convert_error_code(8) ->
    broker_not_available;
convert_error_code(9) ->
    unused;
convert_error_code(10) ->
    message_size_too_large;
convert_error_code(11) ->
    stale_controller_epoch;
convert_error_code(12) ->
    offset_metadata_too_large;
convert_error_code(14) ->
    offsets_load_in_progress;
convert_error_code(15) ->
    consumer_coordinator_not_available;
convert_error_code(16) ->
    not_coordinator_for_consumer.
