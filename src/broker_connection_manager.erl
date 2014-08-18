-module(broker_connection_manager).

-behaviour(gen_server).

-export([start_link/1, get_connection/2]).

%% gen_server callbacks

-export([init/1, handle_call/3]).

-include("api.hrl").

-record(state, { connections, client_id }).
-record(connection_item, { connection, monitor_ref }).

start_link(ClientId) ->
    gen_server:start_link({local, {?MODULE, ClientId}}, ?MODULE, [ClientId], []).

init([ClientId]) ->
    {ok, #state{ connections = dict:new(), client_id = ClientId }}.

get_connection(ClientId, Address) ->
    gen_server:call({local, {?MODULE, ClientId}}, {get_connection, Address}).

handle_call({get_connection, Address}, _From, State = #state{ connections = Connections, client_id = ClientId }) ->
    case dict:find(Address, Connections) of
	{ok, #connection_item{ connection = Connection }} ->
	    {reply, {ok, Connection}, State};
	error ->
	    {ok, Connection} = broker_connection_sup:new_connection(ClientId, Address),
	    MonitorRef = monitor(process, Connection),
	    NewConnections = dict:append(Address, #connection_item{ connection = Connection, monitor_ref = MonitorRef }, Connections),
	    {reply, {ok, Connection}, State#state{ connections = NewConnections }}
    end.
