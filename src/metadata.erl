-module(metadata).

-export([put_request/1, get_response/1]).

-include("api.hrl").

put_request(#metadata_request{topics = Topics}) ->
    utils:put_array(Topics, fun utils:put_string/1).

get_response(Data) ->
    {Brokers, Data1} = utils:get_array(Data, fun get_broker/1),
    {Topics, Rest} = utils:get_array(Data1, fun get_topic_metadata/1),
    {#metadata_response{brokers = Brokers, topics = Topics}, Rest}.

get_broker(Data) ->
    {NodeId, Data1} = utils:get_int32(Data),
    {Host, Data2} = utils:get_string(Data1),
    {Port, Rest} = utils:get_int32(Data2),
    {#broker{node_id = NodeId, host = Host, port = Port}, Rest}.

get_topic_metadata(Data) ->
    {ErrorCode, Data1} = utils:get_int16(Data),
    {TopicName, Data2} = utils:get_string(Data1),
    {Partitions, Rest} = utils:get_array(Data2, fun get_partition_metadata/1),
    {#topic_metadata{error_code = ErrorCode, topic_name = TopicName, partitions = Partitions}, Rest}.

get_partition_metadata(Data) ->
    {ErrorCode, Data1} = utils:get_int16(Data),
    {PartitionId, Data2} = utils:get_int32(Data1),
    {Leader, Data3} = utils:get_int32(Data2),
    {Replicas, Data4} = utils:get_array(Data3, fun utils:get_int32/1),
    {Isr, Rest} = utils:get_array(Data4, fun utils:get_int32/1),
    {#partition_metadata{error_code = ErrorCode, partition_id = PartitionId, leader = Leader, replicas = Replicas, isr = Isr}, Rest}.
