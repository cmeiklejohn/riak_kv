%% @author Christopher Meiklejohn <christopher.meiklejohn@gmail.com>
%% @copyright 2013 Christopher Meiklejohn.
%% @doc Helper functions for pipeline testing.

-module(riak_kv_cp_benchmark).
-author('Christopher Meiklejohn <christopher.meiklejohn@gmail.com>').

-export([mapper/3,
         reducer/4]).

%% @doc Mapper which just returns the same key/value pair.
-spec mapper(term(), term(), term()) -> {ok, {term(), term()}}.
mapper({Key, Value}, Partition, FD) ->
    lager:warning("Mapper received: ~p", [{Key, Value}]),
    Size = byte_size(Value),
    Result = {{Key, Size}, Value},
    ok = riak_pipe_vnode_worker:send_output(Result, Partition, FD),
    lager:warning("Mapper sent: ~p", [Result]),
    ok;
mapper(Input, Partition, FD) ->
    lager:warning("Mapper received: ~p", [Input]),
    ok = riak_pipe_vnode_worker:send_output({1, 1}, Partition, FD),
    lager:warning("Mapper sent: ~p", [{1, 1}]),
    ok.

%% @doc Reducer which just returns the starting accumulator.
-spec reducer(term(), term(), term(), term()) -> {ok, list()}.
reducer(Input, InAcc, Partition, FD) ->
    lager:warning("Reducer received: ~p", [Input]),
    ok = riak_pipe_vnode_worker:send_output(Input, Partition, FD),
    lager:warning("Reducer sent: ~p", [Input]),
    {ok, InAcc}.
