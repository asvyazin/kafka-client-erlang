-module(utils).

-export([get_int16/1, get_int32/1, get_string/1, get_bytes/1, get_array/2]).
-export([put_array/2, put_string/1]).

get_int16(<<Value:16/integer-big, Rest/binary>>) ->
    {Value, Rest}.

get_int32(<<Value:32/integer-big, Rest/binary>>) ->
    {Value, Rest}.

get_string(<<Length:16/integer-big, Data:Length/binary, Rest/binary>>) ->
    {Data, Rest}.

get_bytes(<<Length:32/integer-big, Data:Length/binary, Rest/binary>>) ->
    {Data, Rest}.

get_array(<<ArrayLength:32/integer-big, Rest/binary>>, GetElemFunc) ->
    get_array_elems(Rest, ArrayLength, GetElemFunc, []).

get_array_elems(Rest, 0, _GetElemFunc, Result) ->
    {lists:reverse(Result), Rest};
get_array_elems(Data, ArrayLength, GetElemFunc, Result) ->
    {Elem, Rest} = GetElemFunc(Data),
    get_array_elems(Rest, ArrayLength - 1, GetElemFunc, [Elem | Result]).

put_array(Elems, PutElemFunc) ->
    Len = lists:length(Elems),
    put_array_elems(Elems, PutElemFunc, [<<Len:32/integer-big>>]).

put_array_elems([], _PutElemFunc, Result) ->
    lists:reverse(Result);
put_array_elems([Elem | Tail], PutElemFunc, Result) ->
    put_array_elems(Tail, PutElemFunc, [PutElemFunc(Elem) | Result]).

put_string(Str) ->
    Len = byte_size(Str),
    [<<Len:16/integer-big>>, Str].
