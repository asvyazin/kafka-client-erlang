-module(broker_connection_tests).

-include_lib("eunit/include/eunit.hrl").
-include("api.hrl").

-define(CLIENT_ID, <<"testClientId">>).

metadata_test_() ->
    {ok, Pid} = broker_connection:start_link("localhost", 9092),
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
		      max_number_of_offsets = 1}]}]}))].
