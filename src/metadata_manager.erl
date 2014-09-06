-module(metadata_manager).
-author('Alexander Svyazin <guybrush@live.ru>').

%% API
-export([start_link/2, get_address/3]).

-behaviour(gen_server).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2]).

%% internal
-export([update_metadata_for_topic/4]).

-include("api.hrl").

-record(state, { bootstrap_addresses, topic_to_address, waiting_for_metadata, client_id, node_id_to_address }).

start_link(ClientId, BootstrapAddresses) ->
    gen_server:start_link(?MODULE, [ClientId, BootstrapAddresses], []).

get_address(Pid, Topic, PartitionId) ->
    gen_server:call(Pid, {get_address, Topic, PartitionId}).

init([ClientId, BootstrapAddresses]) ->
    {ok, #state{ bootstrap_addresses = BootstrapAddresses
	       , topic_to_address = dict:new()
	       , waiting_for_metadata = dict:new()
	       , client_id = ClientId
	       , node_id_to_address = dict:new() }}.

handle_call({get_address, Topic, PartitionId}, From, State = #state{ topic_to_address = Topic2Address }) ->
    case dict:find({Topic, PartitionId}, Topic2Address) of
	{ok, Address} ->
	    {reply, {ok, Address}, State};
	error ->
	    maybe_begin_update_metadata_for_topic(Topic, PartitionId, From, State)
    end.

handle_cast({metadata_received, #metadata_response{ brokers = Brokers, topics = Topics }}
	   , State = #state{ node_id_to_address = NodeId2Address, topic_to_address = Topic2Address, waiting_for_metadata = WaitingForMetadata }) ->
    NewNodeId2Address = update_brokers(Brokers, NodeId2Address),
    NewTopic2Address = update_topics(Topics, Topic2Address, NewNodeId2Address),
    NewWaitingForMetadata = lists:foldl(fun (T, W) ->
						case dict:find(T, W) of
						    {ok, Waiters} -> lists:foreach(fun ({Pid, PartitionId}) ->
											   case dict:find({T, PartitionId}, Topic2Address) of
											       {ok, Address} -> gen_server:reply(Pid, {ok, Address});
											       _ -> ok
											   end
										   end, Waiters);
						    _ -> ok
						end,
						dict:erase(T, W)
					end, WaitingForMetadata, Topics),
    {noreply, State#state{ node_id_to_address = NewNodeId2Address, topic_to_address = NewTopic2Address, waiting_for_metadata = NewWaitingForMetadata }}.

maybe_begin_update_metadata_for_topic(Topic, PartitionId, From, State = #state{ waiting_for_metadata = WaitingForMetadata
									      , client_id = ClientId
									      , bootstrap_addresses = BootstrapAddresses }) ->
    case dict:find(Topic, WaitingForMetadata) of
	{ok, _} -> ok;
	error ->
	    spawn_link(?MODULE, update_metadata_for_topic, [Topic, ClientId, BootstrapAddresses, self()])
    end,
    NewWaitingForMetadata = dict:append_list(Topic, [{From, PartitionId}], WaitingForMetadata),
    {noreply, State#state{ waiting_for_metadata = NewWaitingForMetadata }}.

update_metadata_for_topic(Topic, ClientPid, BootstrapAddresses, MyPid) ->
    ActiveConnections = kafka_client:get_active_connections(ClientPid),
    Req = #metadata_request{ topics = [Topic] },
    {ok, Metadata} = case try_update_metadata_from_active_connections(ClientPid, Req, ActiveConnections) of
			 {ok, MetadataResponse} -> {ok, MetadataResponse};
			 _ -> try_update_metadata_from_bootstrap(ClientPid, Req, BootstrapAddresses)
		     end,
    gen_server:cast(MyPid, {metadata_received, Metadata}).

try_update_metadata_from_active_connections(_ClientId, _Req, []) ->
    {error, no_active_connections};
try_update_metadata_from_active_connections(ClientId, Req, [Conn | Conns]) ->
    case broker_connection:metadata(Conn, ClientId, Req) of
	{ok, Resp} ->
	    {ok, Resp};
	_ ->
	    try_update_metadata_from_active_connections(ClientId, Req, Conns)
    end.

try_update_metadata_from_bootstrap(_ClientId, _Req, []) ->
    {error, no_bootstrap_addresses};
try_update_metadata_from_bootstrap(ClientId, Req, [Address | Addresses]) ->
    case broker_connection_manager:get_connection(ClientId, Address) of
	{ok, Connection} ->
	    case broker_connection:metadata(Connection, ClientId, Req) of
		{ok, Resp} ->
		    {ok, Resp};
		_ ->
		    try_update_metadata_from_bootstrap(ClientId, Req, Addresses)
	    end;
	_ ->
	    try_update_metadata_from_bootstrap(ClientId, Req, Addresses)
    end.

update_brokers(Brokers, NodeId2Address) ->
    lists:foldl(fun (#broker{ node_id = NodeId, host = Host, port = Port }, M) ->
			dict:append(NodeId, #broker_address{ host = Host, port = Port }, M)
		end, NodeId2Address, Brokers).

update_topics(Topics, Topic2Address, NodeId2Address) ->
    Topic2NodeId = dict:from_list([ {{Topic, Partition}, NodeId} ||
				      #topic_metadata{ error_code = ok, topic_name = Topic, partitions = Partitions } <- Topics,
				      #partition_metadata{ error_code = ok, partition_id = Partition, leader = NodeId } <- Partitions ]),
    T2A = dict:map(fun (_, N) ->
		     {ok, Address} = dict:find(N, NodeId2Address),
		     Address
		   end, Topic2NodeId),
    dict:merge(fun (_, _, V) -> V end, Topic2Address, T2A).
