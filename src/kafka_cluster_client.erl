-module(kafka_cluster_client).

-author('Alexander Svyazin <guybrush@live.ru>').

-export([produce/6, produce/5, fetch/6, fetch/5,
	 offset/6, offset/5]).

-include("api.hrl").

-define(DEFAULT_REQUIRED_ACKS, 1).

-define(DEFAULT_TIMEOUT, 1000).

-define(DEFAULT_MAX_WAIT_TIME, 10000).

-define(DEFAULT_MIN_BYTES, 1).

-define(DEFAULT_MAX_BYTES, 1024 * 1024).

-define(DEFAULT_MAX_NUMBER_OF_OFFSETS, 10).

produce(ClusterPid, ClientId, Topic, PartitionId,
	Msg) ->
    produce(ClusterPid, ClientId, Topic, PartitionId, Msg,
	    []).

produce(ClusterPid, ClientId, Topic, PartitionId, Msg,
	Options) ->
    {Key, Value} = case Msg of
		     {_, _} -> Msg;
		     _ -> {undefined, Msg}
		   end,
    RequiredAcks = proplists:get_value(required_acks,
				       Options, ?DEFAULT_REQUIRED_ACKS),
    Timeout = proplists:get_value(timeout, Options,
				  ?DEFAULT_TIMEOUT),
    Req = #produce_request{required_acks = RequiredAcks,
			   timeout = Timeout,
			   topics =
			       [#produce_request_topic{topic_name = Topic,
						       partitions =
							   [#produce_request_partition{partition_id
											   =
											   PartitionId,
										       message_set
											   =
											   [#message{offset
													 =
													 -1,
												     key
													 =
													 Key,
												     value
													 =
													 Value}]}]}]},
    do_request(ClusterPid, ClientId, Topic, PartitionId,
	       Req, fun do_produce/5).

fetch(ClusterPid, ClientId, Topic, PartitionId,
      Offset) ->
    fetch(ClusterPid, ClientId, Topic, PartitionId, Offset,
	  []).

fetch(ClusterPid, ClientId, Topic, PartitionId, Offset,
      Options) ->
    MaxWaitTime = proplists:get_value(max_wait_time,
				      Options, ?DEFAULT_MAX_WAIT_TIME),
    MinBytes = proplists:get_value(min_bytes, Options,
				   ?DEFAULT_MIN_BYTES),
    MaxBytes = proplists:get_value(max_bytes, Options,
				   ?DEFAULT_MAX_BYTES),
    Req = #fetch_request{broker_id = -1,
			 max_wait_time = MaxWaitTime, min_bytes = MinBytes,
			 topics =
			     [#fetch_request_topic{topic_name = Topic,
						   partitions =
						       [#fetch_request_partition{partition_id
										     =
										     PartitionId,
										 fetch_offset
										     =
										     Offset,
										 max_bytes
										     =
										     MaxBytes}]}]},
    do_request(ClusterPid, ClientId, Topic, PartitionId,
	       Req, fun do_fetch/5).

offset(ClusterPid, ClientId, Topic, PartitionId,
       Time) ->
    offset(ClusterPid, ClientId, Topic, PartitionId, Time,
	   []).

offset(ClusterPid, ClientId, Topic, PartitionId, Time,
       Options) ->
    MaxNumberOfOffsets =
	proplists:get_value(max_number_of_offsets, Options,
			    ?DEFAULT_MAX_NUMBER_OF_OFFSETS),
    Req = #offset_request{broker_id = -1,
			  topics =
			      [#offset_request_topic{topic_name = Topic,
						     partitions =
							 [#offset_request_partition{partition_id
											=
											PartitionId,
										    time
											=
											Time,
										    max_number_of_offsets
											=
											MaxNumberOfOffsets}]}]},
    do_request(ClusterPid, ClientId, Topic, PartitionId,
	       Req, fun do_offset/5).

do_produce(ClusterPid, ClientId, Topic, PartitionId,
	   Req) ->
    {ok, ConnectionPid} = get_connection(ClusterPid,
					 ClientId, Topic, PartitionId),
    {ok,
     #produce_response{topics =
			   [#produce_response_topic{topic_name = Topic,
						    partitions =
							[#produce_response_partition{partition_id
											 =
											 PartitionId,
										     error_code
											 =
											 ErrorCode,
										     offset
											 =
											 Offset}]}]}} =
	kafka_broker_connection:produce(ConnectionPid, ClientId,
					Req),
    check_error_code(ErrorCode, Offset).

do_fetch(ClusterPid, ClientId, Topic, PartitionId,
	 Req) ->
    {ok, ConnectionPid} = get_connection(ClusterPid,
					 ClientId, Topic, PartitionId),
    {ok,
     #fetch_response{topics =
			 [#fetch_response_topic{topic_name = Topic,
						partitions =
						    [#fetch_response_partition{partition_id
										   =
										   PartitionId,
									       error_code
										   =
										   ErrorCode,
									       highwater_mark_offset
										   =
										   HighwaterMarkOffset,
									       message_set
										   =
										   MessageSet}]}]}} =
	kafka_broker_connection:fetch(ConnectionPid, ClientId,
				      Req),
    check_error_code(ErrorCode,
		     {HighwaterMarkOffset, MessageSet}).

do_offset(ClusterPid, ClientId, Topic, PartitionId,
	  Req) ->
    {ok, ConnectionPid} = get_connection(ClusterPid,
					 ClientId, Topic, PartitionId),
    {ok,
     #offset_response{topics =
			  [#offset_response_topic{topic_name = Topic,
						  partitions =
						      [#offset_response_partition{partition_id
										      =
										      PartitionId,
										  error_code
										      =
										      ErrorCode,
										  offsets
										      =
										      Offsets}]}]}} =
	kafka_broker_connection:offset(ConnectionPid, ClientId,
				       Req),
    check_error_code(ErrorCode, Offsets).

get_connection(ClusterPid, ClientId, Topic,
	       PartitionId) ->
    {ok, Address} = metadata_manager:get_address(ClusterPid,
						 ClientId, Topic, PartitionId),
    case
      broker_connection_manager:get_connection(ClusterPid,
					       Address)
	of
      {ok, Conn} -> {ok, Conn};
      _ ->
	  ok = metadata_manager:force_update_metadata(ClusterPid,
						      ClientId, Topic),
	  get_connection(ClusterPid, ClientId, Topic, PartitionId)
    end.

do_request(ClusterPid, ClientId, Topic, PartitionId,
	   Req, Fun) ->
    case Fun(ClusterPid, ClientId, Topic, PartitionId, Req)
	of
      {ok, Result} -> {ok, Result};
      {error, not_leader_for_partition} ->
	  lager:info("stale node, force update metadata for "
		     "topic ~p",
		     [Topic]),
	  ok = metadata_manager:force_update_metadata(ClusterPid,
						      ClientId, Topic),
	  Fun(ClusterPid, ClientId, Topic, PartitionId, Req);
      Error -> Error
    end.

check_error_code(ErrorCode, Result) ->
    case ErrorCode of
      ok -> {ok, Result};
      _ -> {error, ErrorCode}
    end.