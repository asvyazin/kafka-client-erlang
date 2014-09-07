-module(broker_connection_tests).

-include_lib("eunit/include/eunit.hrl").
-include("api.hrl").

-define(CLIENT_ID, <<"testClientId">>).

kafka_client_test_() ->
    { setup
    , fun () ->
	      ok = kafka_client:start(),
	      {ok, ClientSup} = kafka_client:new_cluster_client([#broker_address{ host = <<"localhost">>, port = 9092 }]),
	      ClientSup
      end
    , fun (_ClientSup) ->
	      application:stop(kafka_client)
      end
    , fun (ClientSup) ->
	      [ ?_assertMatch({ok, _}, metadata_manager:get_address(ClientSup, ?CLIENT_ID, <<"test">>, 0))
	      , ?_assertMatch({ok, _}, kafka_cluster_client:produce(ClientSup, ?CLIENT_ID, <<"test">>, 0, {<<"key">>, <<"value">>}))
	      , ?_assertMatch({ok, _, _}, kafka_cluster_client:fetch(ClientSup, ?CLIENT_ID, <<"test">>, 0, 0))
	      , ?_assertMatch({ok, _}, kafka_cluster_client:offset(ClientSup, ?CLIENT_ID, <<"test">>, 0, 0))]
      end
    }.

broker_connection_test_() ->
    {ok, Pid} = broker_connection:start_link(#broker_address{ host = <<"localhost">>, port = 9092 }),
    [?_assertMatch(#metadata_response{}, broker_connection:metadata(Pid, ?CLIENT_ID, #metadata_request{ topics = [<<"test">>] })),
     ?_assertMatch(#produce_response{},
        broker_connection:produce(Pid, ?CLIENT_ID,
	  #produce_request {
	     required_acks = 1,
	     timeout = 1000,
	     topics = [
	       #produce_request_topic {
		  topic_name = <<"test">>,
		  partitions = [
		    #produce_request_partition {
		       partition_id = 0,
		       message_set = [
			 #message {
			    offset = -1,
			    key = <<"key">>,
			    value = <<"value">>}]}]}]})),
     ?_assertMatch(#fetch_response{},
	broker_connection:fetch(Pid, ?CLIENT_ID,
	  #fetch_request {
	     broker_id = -1,
	     max_wait_time = 100,
	     min_bytes = 1000,
	     topics = [
	       #fetch_request_topic {
		  topic_name = <<"test">>,
		  partitions = [
                    #fetch_request_partition {
		       partition_id = 0,
		       fetch_offset = 0,
		       max_bytes = 10000}]}]})),
    ?_assertMatch(#offset_response{},
       broker_connection:offset(Pid, ?CLIENT_ID,
	 #offset_request {
	    broker_id = -1,
	    topics = [
	      #offset_request_topic {
		 topic_name = <<"test">>,
		 partitions = [
		   #offset_request_partition {
		      partition_id = 0,
		      time = -1,
		      max_number_of_offsets = 1}]}]})),
%     ?_assertMatch(#consumer_metadata_response{},
%        broker_connection:consumer_metadata(Pid, ?CLIENT_ID, #consumer_metadata_request{ consumer_group = <<"testConsumerGroup">> })),
     ?_assertMatch(#offset_commit_response{},
        broker_connection:offset_commit(Pid, ?CLIENT_ID,
	  #offset_commit_request {
	     consumer_group = <<"testConsumerGroup">>,
	     topics = [
	       #offset_commit_request_topic {
		  topic_name = <<"test">>,
		  partitions = [
                    #offset_commit_request_partition {
		       partition_id = 0,
		       offset = 100,
		       timestamp = 100,
		       metadata = <<"metadata">>}]}]})),
     ?_assertMatch(#offset_fetch_response{},
         broker_connection:offset_fetch(Pid, ?CLIENT_ID,
           #offset_fetch_request {
	      consumer_group = <<"testConsumergroup">>,
	      topics = [
                #offset_fetch_request_topic {
		   topic_name = <<"test">>,
		   partitions = [
                     #offset_fetch_request_partition {
			partition_id = 0}]}]}))].
