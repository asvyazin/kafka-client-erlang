-module(broker_connection_manager).

-behaviour(gen_server).

-export([start_link/1, get_connection/2, get_active_connections/1, stop/1]).

%% gen_server callbacks

-export([init/1, handle_call/3, handle_info/2, handle_cast/2]).

-include("api.hrl").

-record(state, { connections, monitors, client_id }).
-record(connection_item, { connection, monitor_ref }).

start_link(ClientId) ->
    gen_server:start_link({global, {?MODULE, ClientId}}, ?MODULE, [ClientId], []).

init([ClientId]) ->
    {ok, #state{ connections = dict:new(), monitors = dict:new(), client_id = ClientId }}.

get_connection(ClientId, Address) ->
    gen_server:call({global, {?MODULE, ClientId}}, {get_connection, Address}).

get_active_connections(ClientId) ->
    gen_server:call({global, {?MODULE, ClientId}}, get_active_connections).

stop(ClientId) ->
    gen_server:cast({global, {?MODULE, ClientId}}, stop).

handle_call({get_connection, Address}, _From, State = #state{ connections = Connections, monitors = Monitors, client_id = _ClientId }) ->
    case dict:find(Address, Connections) of
	{ok, #connection_item{ connection = Connection }} ->
	    {reply, {ok, Connection}, State};
	error ->
	    {ok, Connection} = broker_connection_sup:new_connection(Address),
	    MonitorRef = monitor(process, Connection),
	    NewConnections = dict:store(Address, #connection_item{ connection = Connection, monitor_ref = MonitorRef }, Connections),
	    NewMonitors = dict:store(MonitorRef, Address, Monitors),
	    {reply, {ok, Connection}, State#state{ connections = NewConnections, monitors = NewMonitors }}
    end;
handle_call(get_active_connections, _From, State = #state{ connections = Connections }) ->
    {reply, {ok, [Conn || {_, #connection_item{ connection = Conn }} <- dict:to_list(Connections)]}, State}.

handle_cast(stop, State) ->
    {stop, normal, State}.

handle_info({'DOWN', MonitorRef, process, _ConnPid, _Info}, State = #state{ connections = Connections, monitors = Monitors }) ->
    {ok, Address} = dict:find(MonitorRef, Monitors),
    NewConnections = dict:erase(Address, Connections),
    NewMonitors = dict:erase(MonitorRef, Monitors),
    {noreply, State#state{ connections = NewConnections, monitors = NewMonitors }}.
