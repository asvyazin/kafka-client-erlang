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
