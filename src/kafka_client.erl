-module(kafka_client).

-author('Alexander Svyazin <guybrush@live.ru>').

-behaviour(application).

-export([start/2, stop/1, new_cluster_client/1]).

start(normal, []) ->
    {ok, ClientId} = application:get_env(client_id),
    kafka_client_sup:start_link(ClientId).

stop([]) ->
    ok.

new_cluster_client(BootstrapAddresses) ->
    supervisor:start_child(kafka_client_sup, [BootstrapAddresses]).
