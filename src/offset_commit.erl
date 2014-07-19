-module(offset_commit).

-compile({parse_transform, do}).
-compile({parse_transform, cut}).

-export([put_request/2, get_response/1]).

-include("api.hrl").

put_request(Put, #offset_commit_request{ consumer_group = ConsumerGroup, topics = Topics }) ->
    do([Put || put:string(Put, ConsumerGroup),
	       put:array(Put, Topics, put_request_topic(Put, _))]).

put_request_topic(Put, #offset_commit_request_topic{ topic_name = TopicName, partitions = Partitions }) ->
    do([Put || put:string(Put, TopicName),
	       put:array(Put, Partitions, put_request_partition(Put, _))]).

put_request_partition(Put, #offset_commit_request_partition{ partition_id = PartitionId, offset = Offset, timestamp = Timestamp, metadata = Metadata }) ->
    do([Put || put:int32_big(Put, PartitionId),
	       put:int64_big(Put, Offset),
	       put:int64_big(Put, Timestamp),
	       put:string(Put, Metadata)]).

get_response(Get) ->
    do([Get || Topics <- get:array(Get, get_response_topic(Get)),
	       return(#offset_commit_response{ topics = Topics })]).

get_response_topic(Get) ->
    do([Get || TopicName <- get:string(Get),
	       Partitions <- get:array(Get, get_response_partition(Get)),
	       return(#offset_commit_response_topic{ topic_name = TopicName, partitions = Partitions })]).

get_response_partition(Get) ->
    do([Get || PartitionId <- get:int32_big(Get),
	       ErrorCode <- get:error_code(Get),
	       return(#offset_commit_response_partition{ partition_id = PartitionId, error_code = ErrorCode })]).
