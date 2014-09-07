-module(kafka_client).

-author('Alexander Svyazin <guybrush@live.ru>').

-behaviour(application).

-export([start/0, start/2, stop/1, new_cluster_client/1]).

start() ->
    lager:start(),
    application:start(kafka_client).

start(normal, []) ->
    kafka_client_sup:start_link().

stop([]) ->
    ok.

new_cluster_client(BootstrapAddresses) ->
    supervisor:start_child(kafka_client_sup, [BootstrapAddresses]).
