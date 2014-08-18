-module(broker_connection_sup).

-author('Alexander Svyazin <guybrush@live.ru>').

-behaviour(supervisor).

% api
-export([start_link/1, new_connection/2]).

% supervisor callback
-export([init/1]).

start_link(ClientId) ->
    supervisor:start_link({local, {?MODULE, ClientId}}, ?MODULE, []).

new_connection(ClientId, Address) ->
    supervisor:start_child({local, {?MODULE, ClientId}}, [Address]).

init([]) ->
    {ok, {{simple_one_for_one, 1, 1}
	 , [{broker_connection
	    , {broker_connection, start_link, []}
	    , temporary
	    , 5000
	    , worker
	    , [broker_connection]}]}}.
