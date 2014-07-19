-module(offset_fetch).

-compile({parse_transform, do}).
-compile({parse_transform, cut}).

-export([put_request/2, get_response/1]).

-include("api.hrl").

put_request(Put, #offset_fetch_request{ consumer_group = ConsumerGroup, topics = Topics }) ->
    do([Put || put:string(Put, ConsumerGroup),
	       put:array(Put, Topics, put_request_topic(Put, _))]).

put_request_topic(Put, #offset_fetch_request_topic{ topic_name = TopicName, partitions = Partitions }) ->
    do([Put || put:string(Put, TopicName),
	       put:array(Put, Partitions, put_request_partition(Put, _))]).

put_request_partition(Put, #offset_fetch_request_partition{ partition_id = PartitionId }) ->
    put:int32_big(Put, PartitionId).

get_response(Get) ->
    do([Get || Topics <- get:array(Get, get_response_topic(Get)),
	       return(#offset_fetch_response{ topics = Topics })]).

get_response_topic(Get) ->
    do([Get || TopicName <- get:string(Get),
	       Partitions <- get:array(Get, get_response_partition(Get)),
	       return(#offset_fetch_response_topic{ topic_name = TopicName, partitions = Partitions })]).

get_response_partition(Get) ->
    do([Get || PartitionId <- get:int32_big(Get),
	       Offset <- get:int64_big(Get),
	       Metadata <- get:string(Get),
	       ErrorCode <- get:error_code(Get),
	       return(#offset_fetch_response_partition{ partition_id = PartitionId, offset = Offset, metadata = Metadata, error_code = ErrorCode })]).
