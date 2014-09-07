-module(kafka_cluster_client_sup).

-author('Alexander Svyazin <guybrush@live.ru>').

-behaviour(supervisor).

-export([start_link/1]).

%% supervisor api
-export([init/1]).

start_link(BootstrapAddresses) ->
    supervisor:start_link(?MODULE, [BootstrapAddresses]).

init([BootstrapAddresses]) ->
    {ok, {{one_for_one, 1, 1},
	  [{ broker_connection_manager
	   , {broker_connection_manager, start_link, [self()]}
	   , transient
	   , 5000
	   , worker
	   , [broker_connection_manager] },

	   { metadata_manager
	   , {metadata_manager, start_link, [BootstrapAddresses, self()]}
	   , transient
	   , 5000
	   , worker
	   , [metadata_manager] }]}}.
