%% @author Christopher Meiklejohn <christopher.meiklejohn@gmail.com>
%% @copyright 2013 Christopher Meiklejohn.
%% @doc Helper functions for pipeline testing.

-module(csm_streaming_test).
-author('Christopher Meiklejohn <christopher.meiklejohn@gmail.com>').

-export([mapper/3,
         reducer/4]).

%% @doc Mapper which just returns the same key/value pair.
-spec mapper(term(), term(), term()) -> {ok, {term(), term()}}.
mapper(_Input, Partition, FD) ->
    ok = riak_pipe_vnode_worker:send_output({1, 1}, Partition, FD),
    ok.

%% @doc Reducer which just returns the starting accumulator.
-spec reducer(term(), term(), term(), term()) -> {ok, list()}.
reducer(_, InAcc, _, _) ->
    {ok, InAcc}.
