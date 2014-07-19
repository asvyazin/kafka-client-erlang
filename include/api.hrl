% Raw request

-record(raw_request, {api_key, api_version, correlation_id, client_id, request_bytes}).

%% Message

-define(MESSAGE_MAGIC_BYTE, 0).
-record(message, {offset, key, value}).

%% Metadata

-define(METADATA_API_VERSION, 0).

-record(metadata_request, {topics}).

-record(metadata_response, {brokers, topics}).
-record(broker, {node_id, host, port}).
-record(topic_metadata, {error_code, topic_name, partitions}).
-record(partition_metadata, {error_code, partition_id, leader, replicas, isr}).

%% Produce

-define(PRODUCE_API_VERSION, 0).

-record(produce_request, {required_acks, timeout, topics}).
-record(produce_request_topic, {topic_name, partitions}).
-record(produce_request_partition, {partition_id, message_set}).

-record(produce_response, {topics}).
-record(produce_response_topic, {topic_name, partitions}).
-record(produce_response_partition, {partition_id, error_code, offset}).

%% Fetch

-define(FETCH_API_VERSION, 0).

-record(fetch_request, {broker_id, max_wait_time, min_bytes, topics}).
-record(fetch_request_topic, {topic_name, partitions}).
-record(fetch_request_partition, {partition_id, fetch_offset, max_bytes}).

-record(fetch_response, {topics}).
-record(fetch_response_topic, {topic_name, partitions}).
-record(fetch_response_partition, {partition_id, error_code, highwater_mark_offset, message_set}).

%% Offset

-define(OFFSET_API_VERSION, 0).

-record(offset_request, {broker_id, topics}).
-record(offset_request_topic, {topic_name, partitions}).
-record(offset_request_partition, {partition_id, time, max_number_of_offsets}).

-record(offset_response, {topics}).
-record(offset_response_topic, {topic_name, partitions}).
-record(offset_response_partition, {partition_id, error_code, offsets}).

% Consumer metadata

-define(CONSUMER_METADATA_API_VERSION, 0).

-record(consumer_metadata_request, {consumer_group}).

-record(consumer_metadata_response, {error_code, coordinator_id, coordinator_host, coordinator_port}).

% Offset commit

-define(OFFSET_COMMIT_API_VERSION, 0).

-record(offset_commit_request, {consumer_group, topics}).
-record(offset_commit_request_topic, {topic_name, partitions}).
-record(offset_commit_request_partition, {partition_id, offset, timestamp, metadata}).

-record(offset_commit_response, {topics}).
-record(offset_commit_response_topic, {topic_name, partitions}).
-record(offset_commit_response_partition, {partition_id, error_code}).
