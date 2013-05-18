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
    ok = riak_pipe_vnode_worker:send_output({Key, byte_size(Value)},
                                            Partition, FD),
    ok;
mapper(_Input, Partition, FD) ->
    ok = riak_pipe_vnode_worker:send_output({undefined, 1},
                                            Partition, FD),
    ok.

%% @doc Reducer which just returns the accumulator.
-spec reducer(term(), term(), term(), term()) -> {ok, list()}.
reducer(Key, InAcc, Partition, FD) ->
    Result = [lists:sum(InAcc)],
    ok = riak_pipe_vnode_worker:send_output({Key, Result},
                                            Partition, FD),
    {ok, Result}.
