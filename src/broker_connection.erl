-module(broker_connection).

-behaviour(gen_server).

%% public API
-export([start_link/2, metadata/3, produce/3, fetch/3, offset/3]).

-include("api_key.hrl").
-include("api.hrl").

%% gen_server callbacks
-export([init/1, handle_info/2, handle_call/3]).

-record(state, {socket, current_correlation_id, waiting_requests, put_mod, get_mod}).

-record(request_info, {api_key, from}).

start_link(Address, Port) ->
    gen_server:start_link(?MODULE, [Address, Port], []).

metadata(Pid, ClientId, Req) ->
    gen_server:call(Pid, {metadata, ClientId, Req}).

produce(Pid, ClientId, Req) ->
    gen_server:call(Pid, {produce, ClientId, Req}).

fetch(Pid, ClientId, Req) ->
    gen_server:call(Pid, {fetch, ClientId, Req}).

offset(Pid, ClientId, Req) ->
    gen_server:call(Pid, {offset, ClientId, Req}).

%% gen_server callbacks

init([Address, Port]) ->
    {ok, Sock} = connect(Address, Port),
    {ok, #state{ socket = Sock
	       , current_correlation_id = 0
	       , waiting_requests = []
	       , put_mod = put:new()
	       , get_mod = get:new()}}.

handle_call({metadata, ClientId, Req}, From, State = #state{ put_mod = Put }) ->
    ReqBytes = put:run(Put, metadata:put_request(Put, Req)),
    send_request(#raw_request{ api_key = ?METADATA_REQUEST
			     , api_version = ?METADATA_API_VERSION
			     , client_id = ClientId
			     , request_bytes = ReqBytes }, From, State);
handle_call({produce, ClientId, Req}, From, State = #state{ put_mod = Put }) ->
    ReqBytes = put:run(Put, produce:put_request(Put, Req)),
    send_request(#raw_request{ api_key = ?PRODUCE_REQUEST
			     , api_version = ?PRODUCE_API_VERSION
			     , client_id = ClientId
			     , request_bytes = ReqBytes }, From, State);
handle_call({fetch, ClientId, Req}, From, State = #state{ put_mod = Put }) ->
    ReqBytes = put:run(Put, fetch:put_request(Put, Req)),
    send_request(#raw_request{ api_key = ?FETCH_REQUEST
			     , api_version = ?FETCH_API_VERSION
			     , client_id = ClientId
			     , request_bytes = ReqBytes }, From, State);
handle_call({offset, ClientId, Req}, From, State = #state{ put_mod = Put }) ->
    ReqBytes = put:run(Put, offset:put_request(Put, Req)),
    send_request(#raw_request{ api_key = ?OFFSET_REQUEST
			     , api_version = ?OFFSET_API_VERSION
			     , client_id = ClientId
			     , request_bytes = ReqBytes }, From, State).

send_request(RawRequest, From, State = #state{ socket = Socket
					     , current_correlation_id = CurrentCorrelationId
					     , waiting_requests = WaitingRequests
					     , put_mod = Put}) ->
    RawReqBytes = put:run(Put, raw_request:put(Put, RawRequest#raw_request{ correlation_id = CurrentCorrelationId })),
    ok = gen_tcp:send(Socket, RawReqBytes),
    ReqInfo = #request_info{ api_key = RawRequest#raw_request.api_key, from = From },
    {noreply, State#state{ current_correlation_id = CurrentCorrelationId + 1
			 , waiting_requests = [{CurrentCorrelationId, ReqInfo} | WaitingRequests]}}.
    
handle_info({tcp, Socket, <<CorrelationId:32/integer-big, ResponseData/binary>>}, State = #state{waiting_requests = Requests, get_mod = Get}) ->
    #request_info{api_key = ApiKey, from = From} = proplists:get_value(CorrelationId, Requests),
    gen_server:reply(From, deserialize_response(ApiKey, Get, ResponseData)),
    NewRequests = proplists:delete(CorrelationId, Requests),
    inet:setopts(Socket, [{active, once}]),
    {noreply, State#state{waiting_requests = NewRequests}}.

%% private

connect(Address, Port) ->
    gen_tcp:connect(Address, Port, [binary, {packet, 4}, {active, once}]).

deserialize_response(ApiKey, Get, ResponseData) ->
    Get:eval(do_get(ApiKey, Get), ResponseData).

do_get(?METADATA_REQUEST, Get) ->
    metadata:get_response(Get);
do_get(?PRODUCE_REQUEST, Get) ->
    produce:get_response(Get);
do_get(?FETCH_REQUEST, Get) ->
    fetch:get_response(Get);
do_get(?OFFSET_REQUEST, Get) ->
    offset:get_response(Get).
