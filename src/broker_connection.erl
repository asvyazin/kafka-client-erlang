-module(broker_connection).

-behaviour(gen_server).

%% public API
-export([start_link/1, metadata/3, produce/3, fetch/3, offset/3, consumer_metadata/3, offset_commit/3, offset_fetch/3]).

-include("api_key.hrl").
-include("api.hrl").

%% gen_server callbacks
-export([init/1, handle_info/2, handle_call/3]).

-record(state, {socket, current_correlation_id, waiting_requests, put_mod, get_mod}).

-record(request_info, {api_key, from}).

start_link(Address) ->
    gen_server:start_link(?MODULE, [Address], []).

metadata(Pid, ClientId, Req) ->
    gen_server:call(Pid, {metadata, ClientId, Req}).

produce(Pid, ClientId, Req) ->
    gen_server:call(Pid, {produce, ClientId, Req}).

fetch(Pid, ClientId, Req) ->
    gen_server:call(Pid, {fetch, ClientId, Req}).

offset(Pid, ClientId, Req) ->
    gen_server:call(Pid, {offset, ClientId, Req}).

consumer_metadata(Pid, ClientId, Req) ->
    gen_server:call(Pid, {consumer_metadata, ClientId, Req}).

offset_commit(Pid, ClientId, Req) ->
    gen_server:call(Pid, {offset_commit, ClientId, Req}).

offset_fetch(Pid, ClientId, Req) ->
    gen_server:call(Pid, {offset_fetch, ClientId, Req}).

%% gen_server callbacks

init([Address]) ->
    {ok, Sock} = connect(Address),
    {ok, #state{ socket = Sock
	       , current_correlation_id = 0
	       , waiting_requests = dict:new()
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
			     , request_bytes = ReqBytes }, From, State);
handle_call({consumer_metadata, ClientId, Req}, From, State = #state{ put_mod = Put }) ->
    ReqBytes = put:run(Put, consumer_metadata:put_request(Put, Req)),
    send_request(#raw_request{ api_key = ?CONSUMER_METADATA_REQUEST
			     , api_version = ?CONSUMER_METADATA_API_VERSION
			     , client_id = ClientId
			     , request_bytes = ReqBytes }, From, State);
handle_call({offset_commit, ClientId, Req}, From, State = #state{ put_mod = Put }) ->
    ReqBytes = put:run(Put, offset_commit:put_request(Put, Req)),
    send_request(#raw_request{ api_key = ?OFFSET_COMMIT_REQUEST
			     , api_version = ?OFFSET_COMMIT_API_VERSION
			     , client_id = ClientId
			     , request_bytes = ReqBytes }, From, State);
handle_call({offset_fetch, ClientId, Req}, From, State = #state{ put_mod = Put }) ->
    ReqBytes = put:run(Put, offset_fetch:put_request(Put, Req)),
    send_request(#raw_request{ api_key = ?OFFSET_FETCH_REQUEST
			     , api_version = ?OFFSET_FETCH_API_VERSION
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
			 , waiting_requests = dict:store(CurrentCorrelationId, ReqInfo, WaitingRequests)}}.
    
handle_info({tcp, Socket, <<CorrelationId:32/integer-big, ResponseData/binary>>}, State = #state{waiting_requests = Requests, get_mod = Get}) ->
    #request_info{api_key = ApiKey, from = From} = dict:fetch(CorrelationId, Requests),
    gen_server:reply(From, deserialize_response(ApiKey, Get, ResponseData)),
    NewRequests = dict:erase(CorrelationId, Requests),
    inet:setopts(Socket, [{active, once}]),
    {noreply, State#state{waiting_requests = NewRequests}}.

%% private

connect(#broker_address{ host = Host, port = Port }) ->
    gen_tcp:connect(binary_to_list(Host), Port, [binary, {packet, 4}, {active, once}]).

deserialize_response(ApiKey, Get, ResponseData) ->
    Get:eval(do_get(ApiKey, Get), ResponseData).

do_get(?METADATA_REQUEST, Get) ->
    metadata:get_response(Get);
do_get(?PRODUCE_REQUEST, Get) ->
    produce:get_response(Get);
do_get(?FETCH_REQUEST, Get) ->
    fetch:get_response(Get);
do_get(?OFFSET_REQUEST, Get) ->
    offset:get_response(Get);
do_get(?CONSUMER_METADATA_REQUEST, Get) ->
    consumer_metadata:get_response(Get);
do_get(?OFFSET_COMMIT_REQUEST, Get) ->
    offset_commit:get_response(Get);
do_get(?OFFSET_FETCH_REQUEST, Get) ->
    offset_fetch:get_response(Get).
