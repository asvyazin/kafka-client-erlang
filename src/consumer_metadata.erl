-module(consumer_metadata).

-compile({parse_transform, do}).
-compile({parse_transform, cut}).

-export([put_request/2, get_response/1]).

-include("api.hrl").

put_request(Put, #consumer_metadata_request{ consumer_group = ConsumerGroup }) ->
    put:string(Put, ConsumerGroup).

get_response(Get) ->
    do([Get || ErrorCode <- get:error_code(Get),
	       CoordinatorId <- get:int32_big(Get),
	       CoordinatorHost <- get:string(Get),
	       CoordinatorPort <- get:int32_big(Get),
	       return(#consumer_metadata_response{ error_code = ErrorCode
						 , coordinator_id = CoordinatorId
						 , coordinator_host = CoordinatorHost
						 , coordinator_port = CoordinatorPort })]).
