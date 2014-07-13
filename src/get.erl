-module(get).
-compile({parse_transform, do}).
-export([new/0, int16_big/1, int32_big/1, string/1, bytes/1, array/2]).

new() ->
    state_t:new(identity_m).

int16_big(StateT) ->
    StateT:modify_and_return(fun(<<Value:16/integer-big, Rest/binary>>) ->
				     {Value, Rest}
			     end).

int32_big(StateT) ->
    StateT:modify_and_return(fun(<<Value:32/integer-big, Rest/binary>>) ->
				     {Value, Rest}
			     end).

string(StateT) ->
    StateT:modify_and_return(fun(<<Length:16/integer-big, Data:Length/binary, Rest/binary>>) ->
				     {Data, Rest}
			     end).

bytes(StateT) ->
    StateT:modify_and_return(fun(<<Length:32/integer-big, Data:Length/binary, Rest/binary>>) ->
				     {Data, Rest}
			     end).

array(StateT, G) ->
    do([StateT || Length <- int32_big(StateT),
		  monad:sequence(StateT, lists:duplicate(Length, G))]).
