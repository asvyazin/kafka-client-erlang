-module(kafka_cluster_client).

-author('Alexander Svyazin <guybrush@live.ru>').

-export([produce/6, produce/5]).

-include("api.hrl").
-define(DEFAULT_REQUIRED_ACKS, 1).
-define(DEFAULT_TIMEOUT, 1000).

produce(ClusterPid, ClientId, Topic, PartitionId, Msg) ->
    produce(ClusterPid, ClientId, Topic, PartitionId, Msg, []).

produce(ClusterPid, ClientId, Topic, PartitionId, Msg, Options) ->
    {Key, Value} = case Msg of
		       {_, _} -> Msg;
		       _ -> {undefined, Msg}
		   end,
    RequiredAcks = proplists:get_value(required_acks, Options, ?DEFAULT_REQUIRED_ACKS),
    Timeout = proplists:get_value(timeout, Options, ?DEFAULT_TIMEOUT),
    ReqMessage = #message{ offset = -1, key = Key, value = Value },
    ReqPartition = #produce_request_partition{ partition_id = PartitionId
					     , message_set = [ReqMessage]},
    ReqTopic = #produce_request_topic{ topic_name = Topic
				     , partitions = [ReqPartition]},
    Req = #produce_request{ required_acks = RequiredAcks
			  , timeout = Timeout
			  , topics = [ReqTopic]},
    case do_produce(ClusterPid, ClientId, Topic, PartitionId, Req) of
	{ok, Offset} -> {ok, Offset};
	{not_leader_for_partition, _} ->
	    lager:info("stale node, force update metadata for topic ~p", [Topic]),
	    ok = metadata_manager:force_update_metadata(ClusterPid, ClientId, Topic),
	    case do_produce(ClusterPid, ClientId, Topic, PartitionId, Req) of
		{ok, Offset} -> {ok, Offset};
		{Error, _} -> {error, Error}
	    end;
	{Error, _} -> {error, Error}
    end.

do_produce(ClusterPid, ClientId, Topic, PartitionId, Req) ->
    {ok, Address} = metadata_manager:get_address(ClusterPid, ClientId, Topic, PartitionId),
    {ok, ConnectionPid} = broker_connection_manager:get_connection(ClusterPid, Address),
    #produce_response{
       topics = [
	 #produce_response_topic{
	    topic_name = Topic,
	    partitions = [
	      #produce_response_partition{
		 partition_id = PartitionId,
		 error_code = ErrorCode,
		 offset = Offset }]}]} = broker_connection:produce(ConnectionPid, ClientId, Req),
    {ErrorCode, Offset}.
