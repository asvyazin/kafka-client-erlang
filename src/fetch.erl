-module(fetch).
-compile({parse_transform, do}).
-compile({parse_transform, cut}).

-export([put_request/2, get_response/1]).

-include("api.hrl").

put_request(Put, #fetch_request{ broker_id = BrokerId
			       , max_wait_time = MaxWaitTime
			       , min_bytes = MinBytes
			       , topics = Topics }) ->
    do([Put || put:int32_big(Put, BrokerId),
	       put:int32_big(Put, MaxWaitTime),
	       put:int32_big(Put, MinBytes),
	       put:array(Put, Topics, put_request_topic(Put, _))]).

put_request_topic(Put, #fetch_request_topic{ topic_name = TopicName
					   , partitions = Partitions }) ->
    do([Put || put:string(Put, TopicName),
	       put:array(Put, Partitions, put_request_partition(Put, _))]).

put_request_partition(Put, #fetch_request_partition{ partition_id = PartitionId
						   , fetch_offset = FetchOffset
						   , max_bytes = MaxBytes }) ->
    do([Put || put:int32_big(Put, PartitionId),
	       put:int64_big(Put, FetchOffset),
	       put:int32_big(Put, MaxBytes)]).


get_response(Get) ->
    do([Get || Topics <- get:array(Get, get_response_topic(Get)),
	       return(#fetch_response{ topics = Topics })]).

get_response_topic(Get) ->
    do([Get || TopicName <- get:string(Get),
	       Partitions <- get:array(Get, get_response_partition(Get)),
	       return(#fetch_response_topic{ topic_name = TopicName, partitions = Partitions })]).

get_response_partition(Get) ->
    do([Get || PartitionId <- get:int32_big(Get),
	       ErrorCode <- get:error_code(Get),
	       HighwaterMarkOffset <- get:int64_big(Get),
	       MessageSet <- message_set:get(Get),
	       return(#fetch_response_partition{ partition_id = PartitionId
					       , error_code = ErrorCode
					       , highwater_mark_offset = HighwaterMarkOffset
					       , message_set = MessageSet })]).
