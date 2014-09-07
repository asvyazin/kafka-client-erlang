-module(kafka_client_sup).

-author('Alexander Svyazin <guybrush@live.ru>').

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    {ok, {{simple_one_for_one, 1, 1},
	  [{ kafka_cluster_client_sup
	   , {kafka_cluster_client_sup, start_link, []}
	   , temporary
	   , 5000
	   , supervisor
	   , [kafka_cluster_client_sup] }]}}.
