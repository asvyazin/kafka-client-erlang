-module(raw_request).
-compile({parse_transform, do}).
-compile({parse_transform, cut}).

-include("api.hrl").

-export([put/2]).

put(StateT, #raw_request{ api_key = ApiKey
			, api_version = ApiVersion
			, correlation_id = CorrelationId
			, client_id = ClientId
			, request_bytes = RequestBytes}) ->
    do([StateT || put:int16_big(StateT, ApiKey),
		  put:int16_big(StateT, ApiVersion),
		  put:int32_big(StateT, CorrelationId),
		  put:string(StateT, ClientId),
		  put:bytes_without_prefix(StateT, RequestBytes)]).
