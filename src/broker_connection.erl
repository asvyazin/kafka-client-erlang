-module(broker_connection).

-behaviour(gen_server).

%% public API
-export([start_link/2, metadata/3]).

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
			     , request_bytes = ReqBytes }, From, State).

send_request(RawRequest, From, State = #state{ socket = Socket
					     , current_correlation_id = CurrentCorrelationId
					     , waiting_requests = WaitingRequests
					     , put_mod = Put}) ->
    RawReqBytes = put:run(Put, raw_request:put(Put, RawRequest#raw_request{ correlation_id = CurrentCorrelationId })),
    ok = gen_tcp:send(Socket, RawReqBytes),
    ReqInfo = #request_info{ api_key = ?METADATA_REQUEST, from = From },
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

deserialize_response(?METADATA_REQUEST, Get, ResponseData) ->
    Get:eval(metadata:get_response(Get), ResponseData).
