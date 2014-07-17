-module(offset).

-compile({parse_transform, do}).
-compile({parse_transform, cut}).

-export([put_request/2, get_response/1]).

-include("api.hrl").

put_request(Put, #offset_request{ broker_id = BrokerId, topics = Topics }) ->
    do([Put || put:int32_big(Put, BrokerId),
	       put:array(Put, Topics, put_request_topic(Put, _))]).

put_request_topic(Put, #offset_request_topic{ topic_name = TopicName, partitions = Partitions }) ->
    do([Put || put:string(Put, TopicName),
	       put:array(Put, Partitions, put_request_partition(Put, _))]).

put_request_partition(Put, #offset_request_partition{ partition_id = PartitionId, time = Time, max_number_of_offsets = MaxNumberOfOffsets }) ->
    do([Put || put:int32_big(Put, PartitionId),
	       put:int64_big(Put, Time),
	       put:int32_big(Put, MaxNumberOfOffsets)]).

get_response(Get) ->
    do([Get || Topics <- get:array(Get, get_response_topic(Get)),
	       return(#offset_response{topics = Topics})]).

get_response_topic(Get) ->
    do([Get || TopicName <- get:string(Get),
	       Partitions <- get:array(Get, get_response_partition(Get)),
	       return(#offset_response_topic{topic_name = TopicName, partitions = Partitions})]).

get_response_partition(Get) ->
    do([Get || PartitionId <- get:int32_big(Get),
	       ErrorCode <- get:error_code(Get),
	       Offsets <- get:array(Get, get:int64_big(Get)),
	       return(#offset_response_partition{partition_id = PartitionId, error_code = ErrorCode, offsets = Offsets})]).
