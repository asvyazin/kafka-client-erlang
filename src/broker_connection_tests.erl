-module(broker_connection_tests).

-include_lib("eunit/include/eunit.hrl").
-include("api.hrl").

-define(CLIENT_ID, <<"testClientId">>).

metadata_test() ->
    {ok, Pid} = broker_connection:start_link("localhost", 9092),
    #metadata_response{} = broker_connection:metadata(Pid, ?CLIENT_ID, #metadata_request{ topics = [<<"test">>] }).
