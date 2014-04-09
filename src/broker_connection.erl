-module(broker_connection).

-behaviour(gen_server).

%% public API
-export([start_link/2]).

-include("api_key.hrl").
-include("api.hrl").

%% gen_server callbacks
-export([init/1, handle_info/2]).

-record(state, {socket, current_correlation_id, waiting_requests}).

-record(request_info, {api_key, sender_pid}).

start_link(Address, Port) ->
    gen_server:start_link(?MODULE, [Address, Port], []).

%% gen_server callbacks

init([Address, Port]) ->
    {ok, Sock} = connect(Address, Port),
    {ok, #state{socket = Sock, current_correlation_id = 0, waiting_requests = []}}.

handle_info({tcp, _Socket, <<CorrelationId:32/integer-big, ResponseData/binary>>}, State = #state{waiting_requests = Requests}) ->
    #request_info{api_key = ApiKey, sender_pid = SenderPid} = proplists:get_value(CorrelationId, Requests),
    SenderPid ! deserialize_response(ApiKey, ResponseData),
    NewRequests = proplists:delete(CorrelationId, Requests),
    {noreply, State#state{waiting_requests = NewRequests}}.

%% private

connect(Address, Port) ->
    gen_tcp:connect(Address, Port, [binary, {packet, 4}, {active, once}]).

deserialize_response(?METADATA_REQUEST, ResponseData) ->
    {MetadataResponse, <<>>} = metadata:get_response(ResponseData),
    MetadataResponse.
