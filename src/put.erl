-module(put).
-compile({parse_transform, do}).

-export([new/0, run/2, int8/2, int16_big/2, int32_big/2, int64_big/2, string/2, array/3, bytes/2, bytes_without_prefix/2]).

new() ->
    state_t:new(identity_m).

run(StateT, P) ->
    S = StateT:exec(P, []),
    list_to_binary(S).

int8(StateT, I) ->
    StateT:modify(fun (S) -> [S, <<I:8/integer-big>>] end).

int16_big(StateT, I) ->
    StateT:modify(fun (S) -> [S, <<I:16/integer-big>>] end).

int32_big(StateT, I) ->
    StateT:modify(fun (S) -> [S, <<I:32/integer-big>>] end).

int64_big(StateT, I) ->
    StateT:modify(fun (S) -> [S, <<I:64/integer-big>>] end).

string(StateT, Str) ->
    do([StateT || int16_big(StateT, byte_size(Str)),
		  StateT:modify(fun (S) -> [S, Str] end)]).

array(StateT, L, M) ->
    do([StateT || int32_big(StateT, length(L)),
		  monad:sequence(StateT, [M(X) || X <- L])]).

bytes(StateT, Bytes) ->
    do([StateT || int32_big(StateT, byte_size(Bytes)),
		  bytes_without_prefix(StateT, Bytes)]).

bytes_without_prefix(StateT, Bytes) ->
    StateT:modify(fun (S) -> [S, Bytes] end).
