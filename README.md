# About

## Example

```
> kafka_client:start().
> {ok, ClusterPid} = kafka_client:new_cluster_client([#broker_address{host = <<"localhost">>, port = 9092}]).
> ClientId = <<"test_client">>.
> Topic = <<"test">>.
> PartitionId = 0.
> Key = <<"key">>.
> Value = <<"value">>.
> {ok, Offset} = kafka_cluster_client:produce(ClusterPid, ClientId, Topic, PartitionId, {Key, Value}).
```
