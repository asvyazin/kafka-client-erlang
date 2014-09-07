-module(broker_connection_sup).

-author('Alexander Svyazin <guybrush@live.ru>').

-behaviour(supervisor).

% api
-export([start_link/0, new_connection/2]).

% supervisor callback
-export([init/1]).

start_link() ->
    supervisor:start_link(?MODULE, []).

new_connection(Pid, Address) ->
    supervisor:start_child(Pid, [Address]).

init([]) ->
    {ok, {{simple_one_for_one, 1, 1}
	 , [{ broker_connection
	    , {broker_connection, start_link, []}
	    , temporary
	    , 5000
	    , worker
	    , [broker_connection] }]}}.
