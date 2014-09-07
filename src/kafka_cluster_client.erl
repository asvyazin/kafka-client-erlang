-module(kafka_cluster_client).

-author('Alexander Svyazin <guybrush@live.ru>').

-export([produce/6, produce/5, fetch/6, fetch/5, offset/6, offset/5]).

-include("api.hrl").
-define(DEFAULT_REQUIRED_ACKS, 1).
-define(DEFAULT_TIMEOUT, 1000).
-define(DEFAULT_MAX_WAIT_TIME, 10000).
-define(DEFAULT_MIN_BYTES, 1).
-define(DEFAULT_MAX_BYTES, 1024*1024).
-define(DEFAULT_MAX_NUMBER_OF_OFFSETS, 10).

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

fetch(ClusterPid, ClientId, Topic, PartitionId, Offset) ->
    fetch(ClusterPid, ClientId, Topic, PartitionId, Offset, []).

fetch(ClusterPid, ClientId, Topic, PartitionId, Offset, Options) ->
    MaxWaitTime = proplists:get_value(max_wait_time, Options, ?DEFAULT_MAX_WAIT_TIME),
    MinBytes = proplists:get_value(min_bytes, Options, ?DEFAULT_MIN_BYTES),
    MaxBytes = proplists:get_value(max_bytes, Options, ?DEFAULT_MAX_BYTES),
    Req = #fetch_request{
	     broker_id = -1,
	     max_wait_time = MaxWaitTime,
	     min_bytes = MinBytes,
	     topics = [
	       #fetch_request_topic{
		  topic_name = Topic,
		  partitions = [
		    #fetch_request_partition{
		       partition_id = PartitionId,
		       fetch_offset = Offset,
		       max_bytes = MaxBytes}]}]},
    case do_fetch(ClusterPid, ClientId, Topic, PartitionId, Req) of
	{ok, HighwaterMarkOffset, MessageSet} ->
	    {ok, HighwaterMarkOffset, MessageSet};
	{not_leader_for_partition, _, _} ->
	    lager:info("stale node, force update metadata for topic ~p", [Topic]),
	    ok = metadata_manager:force_update_metadata(ClusterPid, ClientId, Topic),
	    case do_fetch(ClusterPid, ClientId, Topic, PartitionId, Req) of
		{ok, HighwaterMarkOffset, MessageSet} ->
		    {ok, HighwaterMarkOffset, MessageSet};
		{Error, _} -> {error, Error}
	    end;
	{Error, _} -> {error, Error}
    end.

offset(ClusterPid, ClientId, Topic, PartitionId, Time) ->
    offset(ClusterPid, ClientId, Topic, PartitionId, Time, []).

offset(ClusterPid, ClientId, Topic, PartitionId, Time, Options) ->
    MaxNumberOfOffsets = proplists:get_value(max_number_of_offsets, Options, ?DEFAULT_MAX_NUMBER_OF_OFFSETS),
    Req = #offset_request{
	     broker_id = -1,
	     topics = [
	       #offset_request_topic{
		  topic_name = Topic,
		  partitions = [
		    #offset_request_partition{
		       partition_id = PartitionId,
		       time = Time,
		       max_number_of_offsets = MaxNumberOfOffsets}]}]},
    case do_offset(ClusterPid, ClientId, Topic, PartitionId, Req) of
	{ok, Offsets} ->
	    {ok, Offsets};
	{not_leader_for_partition, _} ->
	    lager:info("stale node, force update metadata for topic ~p", [Topic]),
	    ok = metadata_manager:force_update_metadata(ClusterPid, ClientId, Topic),
	    case do_offset(ClusterPid, ClientId, Topic, PartitionId, Req) of
		{ok, Offsets} ->
		    {ok, Offsets};
		{Error, _} -> {error, Error}
	    end;
	{Error, _} -> {error, Error}
    end.

do_produce(ClusterPid, ClientId, Topic, PartitionId, Req) ->
    {ok, ConnectionPid} = get_connection(ClusterPid, ClientId, Topic, PartitionId),
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

do_fetch(ClusterPid, ClientId, Topic, PartitionId, Req) ->
    {ok, ConnectionPid} = get_connection(ClusterPid, ClientId, Topic, PartitionId),
    #fetch_response{
       topics = [
	 #fetch_response_topic{
	    topic_name = Topic,
	    partitions = [
	      #fetch_response_partition{
		 partition_id = PartitionId,
		 error_code = ErrorCode,
		 highwater_mark_offset = HighwaterMarkOffset,
		 message_set = MessageSet}]}]} = broker_connection:fetch(ConnectionPid, ClientId, Req),
    {ErrorCode, HighwaterMarkOffset, MessageSet}.

do_offset(ClusterPid, ClientId, Topic, PartitionId, Req) ->
    {ok, ConnectionPid} = get_connection(ClusterPid, ClientId, Topic, PartitionId),
    #offset_response{
       topics = [
	 #offset_response_topic{
	    topic_name = Topic,
	    partitions = [
	      #offset_response_partition{
		 partition_id = PartitionId,
		 error_code = ErrorCode,
		 offsets = Offsets}]}]} = broker_connection:offset(ConnectionPid, ClientId, Req),
    {ErrorCode, Offsets}.

get_connection(ClusterPid, ClientId, Topic, PartitionId) ->
    {ok, Address} = metadata_manager:get_address(ClusterPid, ClientId, Topic, PartitionId),
    broker_connection_manager:get_connection(ClusterPid, Address).
