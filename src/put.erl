-module(put).
-compile({parse_transform, do}).

-export([new/0, run_put/2, int16_big/2, int32_big/2, string/2, array/3]).

new() ->
    state_t:new(identity_m).

run_put(StateT, P) ->
    S = StateT:exec(P, []),
    lists:reverse(S).

int16_big(StateT, I) ->
    StateT:modify(fun (S) -> [<<I:16/integer-big>>, S] end).

int32_big(StateT, I) ->
    StateT:modify(fun (S) -> [<<I:32/integer-big>>, S] end).

string(StateT, Str) ->
    do([StateT || int16_big(StateT, byte_size(Str)),
		  StateT:modify(fun (S) -> [Str, S] end)]).

array(StateT, L, M) ->
    do([StateT || int32_big(StateT, length(L)),
		  monad:sequence(StateT, [M(X) || X <- L])]).
