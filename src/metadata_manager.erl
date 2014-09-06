-module(metadata_manager).
-author('Alexander Svyazin <guybrush@live.ru>').

%% API
-export([start_link/3, get_address/3, stop/1]).

-behaviour(gen_server).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2]).

%% internal
-export([update_metadata_for_topic/5]).

-include("api.hrl").

-record(state, { bootstrap_addresses, topic_to_address, waiting_for_metadata, client_id, cluster_id, node_id_to_address }).

start_link(ClusterId, ClientId, BootstrapAddresses) ->
    gen_server:start_link({global, {?MODULE, ClusterId}}, ?MODULE, [ClusterId, ClientId, BootstrapAddresses], []).

get_address(ClusterId, Topic, PartitionId) ->
    gen_server:call({global, {?MODULE, ClusterId}}, {get_address, Topic, PartitionId}).

stop(ClusterId) ->
    gen_server:cast({global, {?MODULE, ClusterId}}, stop).

init([ClusterId, ClientId, BootstrapAddresses]) ->
    {ok, #state{ bootstrap_addresses = BootstrapAddresses
	       , topic_to_address = dict:new()
	       , waiting_for_metadata = dict:new()
	       , client_id = ClientId
	       , cluster_id = ClusterId
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
    NewWaitingForMetadata = lists:foldl(fun (#topic_metadata{ topic_name = T }, W) ->
						{ok, Waiters} = dict:find(T, W),
						lists:foreach(fun ({Pid, PartitionId}) ->
								      {ok, Address} = dict:find({T, PartitionId}, NewTopic2Address),
								      gen_server:reply(Pid, {ok, Address})
							      end, Waiters),
						dict:erase(T, W)
					end, WaitingForMetadata, Topics),
    {noreply, State#state{ node_id_to_address = NewNodeId2Address, topic_to_address = NewTopic2Address, waiting_for_metadata = NewWaitingForMetadata }};
handle_cast(stop, State) ->
    {stop, normal, State}.

maybe_begin_update_metadata_for_topic(Topic, PartitionId, From, State = #state{ waiting_for_metadata = WaitingForMetadata
									      , client_id = ClientId
									      , cluster_id = ClusterId
									      , bootstrap_addresses = BootstrapAddresses }) ->
    case dict:find(Topic, WaitingForMetadata) of
	{ok, _} -> ok;
	error ->
	    spawn_link(?MODULE, update_metadata_for_topic, [Topic, ClusterId, ClientId, BootstrapAddresses, self()])
    end,
    NewWaitingForMetadata = dict:append_list(Topic, [{From, PartitionId}], WaitingForMetadata),
    {noreply, State#state{ waiting_for_metadata = NewWaitingForMetadata }}.

update_metadata_for_topic(Topic, ClusterId, ClientId, BootstrapAddresses, MyPid) ->
    {ok, ActiveConnections} = broker_connection_manager:get_active_connections(ClusterId),
    Req = #metadata_request{ topics = [Topic] },
    {ok, Metadata} = case try_update_metadata_from_active_connections(ClusterId, Req, ActiveConnections) of
			 {ok, MetadataResponse} -> {ok, MetadataResponse};
			 _ -> try_update_metadata_from_bootstrap(ClusterId, ClientId, Req, BootstrapAddresses)
		     end,
    gen_server:cast(MyPid, {metadata_received, Metadata}).

try_update_metadata_from_active_connections(_ClientId, _Req, []) ->
    {error, no_active_connections};
try_update_metadata_from_active_connections(ClientId, Req, [Conn | _Conns]) ->
    Resp = broker_connection:metadata(Conn, ClientId, Req),
    {ok, Resp}.

try_update_metadata_from_bootstrap(_ClusterId, _ClientId, _Req, []) ->
    {error, no_bootstrap_addresses};
try_update_metadata_from_bootstrap(ClusterId, ClientId, Req, [Address | _Addresses]) ->
    {ok, Connection} = broker_connection_manager:get_connection(ClusterId, Address),
    Resp = broker_connection:metadata(Connection, ClientId, Req),
    {ok, Resp}.

update_brokers(Brokers, NodeId2Address) ->
    lists:foldl(fun (#broker{ node_id = NodeId, host = Host, port = Port }, M) ->
			dict:store(NodeId, #broker_address{ host = Host, port = Port }, M)
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
