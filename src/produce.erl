-module(produce).
-compile({parse_transform, do}).
-compile({parse_transform, cut}).

-export([put_request/2, get_response/1]).

-include("api.hrl").

put_request(Put, #produce_request{ required_acks = RequiredAcks, timeout = Timeout, topics = Topics }) ->
    do([Put || put:int16_big(Put, RequiredAcks),
	       put:int32_big(Put, Timeout),
	       put:array(Put, Topics, put_request_topic(Put, _))]).

put_request_topic(Put, #produce_request_topic{ topic_name = TopicName, partitions = Partitions }) ->
    do([Put || put:string(Put, TopicName),
	       put:array(Put, Partitions, put_request_partition(Put, _))]).

put_request_partition(Put, #produce_request_partition{ partition_id = PartitionId, message_set = MessageSet }) ->
    do([Put || put:int32_big(Put, PartitionId),
	       MessageSetBytes = put:run(Put, message_set:put(Put, MessageSet)),
	       put:bytes(MessageSetBytes)]).

get_response(Get) ->
    do([Get || Topics <- get:array(Get, get_response_topic(Get)),
	       return(#produce_response{ topics = Topics })]).

get_response_topic(Get) ->
    do([Get || TopicName <- get:string(Get),
	       Partitions <- get:array(Get, get_response_partition(Get)),
	       return(#produce_response_topic{ topic_name = TopicName, partitions = Partitions })]).

get_response_partition(Get) ->
    do([Get || PartitionId <- get:int32_big(Get),
	       ErrorCode <- get:int16_big(Get),
	       Offset <- get:int64_big(Get),
	       return(#produce_response_partition{ partition_id = PartitionId, error_code = ErrorCode, offset = Offset })]).
