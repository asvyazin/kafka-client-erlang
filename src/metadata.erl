-module(metadata).
-compile({parse_transform, do}).
-compile({parse_transform, cut}).

-export([put_request/2, get_response/1]).

-include("api.hrl").

put_request(StateT, #metadata_request{topics = Topics}) ->
    put:array(StateT, Topics, put:string(StateT, _)).

get_response(StateT) ->
    do([StateT || Brokers <- get:array(StateT, get_broker(StateT)),
		  Topics <- get:array(StateT, get_topic_metadata(StateT)),
		  return(#metadata_response{brokers = Brokers, topics = Topics})]).

get_broker(StateT) ->
    do([StateT || NodeId <- get:int32_big(StateT),
		  Host <- get:string(StateT),
		  Port <- get:int32_big(StateT),
		  return(#broker{node_id = NodeId, host = Host, port = Port})]).

get_topic_metadata(StateT) ->
    do([StateT || ErrorCode <- get:error_code(StateT),
		  TopicName <- get:string(StateT),
		  Partitions <- get:array(StateT, get_partition_metadata(StateT)),
		  return(#topic_metadata{error_code = ErrorCode, topic_name = TopicName, partitions = Partitions})]).

get_partition_metadata(StateT) ->
    do([StateT || ErrorCode <- get:error_code(StateT),
		  PartitionId <- get:int32_big(StateT),
		  Leader <- get:int32_big(StateT),
		  Replicas <- get:array(StateT, get:int32_big(StateT)),
		  Isr <- get:array(StateT, get:int32_big(StateT)),
		  return(#partition_metadata{error_code = ErrorCode, partition_id = PartitionId, leader = Leader, replicas = Replicas, isr = Isr})]).
