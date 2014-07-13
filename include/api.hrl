-record(raw_request, {api_key, api_version, correlation_id, client_id, request_bytes}).

%% Metadata

-define(METADATA_API_VERSION, 0).

-record(metadata_request, {topics}).
-record(metadata_response, {brokers, topics}).
-record(broker, {node_id, host, port}).
-record(topic_metadata, {error_code, topic_name, partitions}).
-record(partition_metadata, {error_code, partition_id, leader, replicas, isr}).
