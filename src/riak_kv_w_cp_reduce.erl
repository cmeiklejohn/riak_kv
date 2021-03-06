%% -------------------------------------------------------------------
%%
%% Copyright (c) 2011 Basho Technologies, Inc.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

%% @doc A "reduce"-like fitting (in the MapReduce sense).  Really more
%%      like a keyed list-fold.  This fitting expects inputs of the
%%      form `{Key, Value}'.  For each input, the fitting evaluates a
%%      function (its argument) on the `Value' and any previous result
%%      for that `Key', or `[]' (the empty list) if that `Key' has
%%      never been seen by this worker.  When `done' is finally
%%      received, the fitting sends each key-value pair it has
%%      evaluated as an input to the next fittin.
%%
%%      The intent is that a fitting might receive a stream of inputs
%%      like `{a, 1}, {b, 2}, {a 3}, {b, 4}' and send on results like
%%      `{a, 4}, {b, 6}' by using a simple "sum" function.
%%
%%      This function expects a function as its argument.  The function
%%      should be arity-4 and expect the arguments:
%%<dl><dt>
%%      `Key' :: term()
%%</dt><dd>
%%      Whatever aggregation key is necessary for the algorithm.
%%</dd><dt>
%%      `InAccumulator' :: [term()]
%%</dt><dd>
%%      A list composed of the new input, cons'd on the front of the
%%      result of the last evaluation for this `Key' (or the empty list
%%      if this is the first evaluation for the key).
%%</dd><dt>
%%      `Partition' :: riak_pipe_vnode:partition()
%%</dt><dd>
%%      The partition of the vnode on which this worker is running.
%%      (Useful for logging, or sending other output.)
%%</dd><dt>
%%      `FittingDetails' :: #fitting_details{}
%%</dt><dd>
%%      The details of this fitting.
%%      (Useful for logging, or sending other output.)
%%</dd></dl>
%%
%%      The function should return a tuple of the form `{ok,
%%      NewAccumulator}', where `NewAccumulator' is a list, onto which
%%      the next input will be cons'd.  For example, the function to
%%      sum values for a key, as described above might look like:
%% ```
%% fun(_Key, Inputs, _Partition, _FittingDetails) ->
%%    {ok, [lists:sum(Inputs)]}
%% end
%% '''
%%
%%      The preferred consistent-hash function for this fitting is
%%      {@link chashfun/1}.  It hashes the input `Key'.  Any other
%%      partition function should work, but beware that a function
%%      that sends values for the same `Key' to different partitions
%%      will result in fittings down the pipe receiving multiple
%%      results for the `Key'.
%%
%%      This fitting produces as its archive, the store of evaluation
%%      results for the keys it has seen.  To merge handoff values,
%%      the lists stored with each key are concatenated, and the
%%      reduce function is re-evaluated.
-module(riak_kv_w_cp_reduce).
-behaviour(riak_pipe_vnode_worker).

-export([init/2,
         process/3,
         done/1,
         archive/1,
         handoff/2,
         checkpoint/2,
         validate_arg/1]).

-export([chashfun/1]).

-include_lib("riak_pipe/include/riak_pipe.hrl").
-include_lib("riak_pipe/include/riak_pipe_log.hrl").

-record(state, {accs :: dict(),
                p :: riak_pipe_vnode:partition(),
                fd :: riak_pipe_fitting:details()}).
-opaque state() :: #state{}.

%% @doc Setup creates the store for evaluation results (a dict()) and
%%      stashes away the `Partition' and `FittingDetails' for later.
-spec init(riak_pipe_vnode:partition(),
           riak_pipe_fitting:details()) ->
         {ok, state()}.
init(Partition, FittingDetails) ->

    %% Load checkpoint data from KV.
    Key = checkpoint_key(#state{p=Partition, fd=FittingDetails}),
    Bucket = checkpoint_bucket(),

    Accs = case riak:local_client() of
        {ok, Client} ->
            case Client:get(Bucket, Key) of
                {ok, RObject} ->
                    Object = riak_object:get_value(RObject),
                    deserialize_archive(Object);
                GetError ->
                    lager:warning("Checkpoint retrieval failed. ~p\n",
                                  [GetError]),
                    initial_state()
            end;
        ClientError ->
            lager:warning("Checkpointing: no riak client available. ~p\n",
                          [ClientError]),
            initial_state()
    end,

    {ok, #state{accs=Accs, p=Partition, fd=FittingDetails}}.

%% @doc Process looks up the previous result for the `Key', and then
%%      evaluates the funtion on that with the new `Input'.
-spec process({term(), term()}, boolean(), state()) -> {ok, state()}.
process({Key, Input}, _Last, #state{accs=Accs}=State) ->
    case dict:find(Key, Accs) of
        {ok, OldAcc} -> ok;
        error        -> OldAcc=[]
    end,
    InAcc = [Input|OldAcc],
    case reduce(Key, InAcc, State) of
        {ok, OutAcc} ->
            {ok, State#state{accs=dict:store(Key, OutAcc, Accs)}};
        {error, {Type, Error, Trace}} ->
            %%TODO: forward
            lager:error(
              "~p:~p reducing:~n   ~P~n   ~P",
              [Type, Error, InAcc, 2, Trace, 5]),
            {ok, State}
    end.

%% @doc Unless the aggregation function sends its own outputs, done/1
%%      is where all outputs are sent.
-spec done(state()) -> ok.
done(#state{accs=Accs, p=Partition, fd=FittingDetails}) ->
    _ = [ riak_pipe_vnode_worker:send_output(A, Partition, FittingDetails)
      || A <- dict:to_list(Accs)],
    ok.

%% @doc The archive is just the store (dict()) of evaluation results.
-spec archive(state()) -> {ok, dict()}.
archive(#state{accs=Accs}) ->
    %% just send state of reduce so far
    {ok, Accs}.

%% @doc Checkpoint archived state to riak_kv.  In addition, forward the
%%      accumulated state to the sink.
%% @todo Possibly memoize the client connection.
-spec checkpoint(term(), state()) -> ok.
checkpoint(Archive,
           #state{accs=Accs, p=Partition, fd=FittingDetails} = State) ->

    %% Checkpoint data to KV.
    Key = checkpoint_key(State),
    Bucket = checkpoint_bucket(),
    Value = serialize_archive(Archive),
    Object = riak_object:new(Bucket, Key, Value),

    case riak:local_client() of
        {ok, Client} ->
            case Client:put(Object) of
                ok ->
                    ok;
                PutError ->
                    lager:warning("Checkpoint storage failed. ~p\n",
                                  [PutError])
            end;
        ClientError ->
            lager:warning("Checkpointing: no riak client available. ~p\n",
                          [ClientError]),
            ClientError
    end,

    %% Forward results to the next worker.
    _ = [ riak_pipe_vnode_worker:send_output(A, Partition, FittingDetails)
      || A <- dict:to_list(Accs)],
    ok.

%% @doc The handoff merge is simple a dict:merge, where entries for
%%      the same key are concatenated.  The reduce function is also
%%      re-evaluated for the key, such that {@link done/1} still has
%%      the correct value to send, even if no more inputs arrive.
-spec handoff(dict(), state()) -> {ok, state()}.
handoff(HandoffAccs, #state{accs=Accs}=State) ->
    %% for each Acc, add to local accs;
    NewAccs = dict:merge(fun(K, HA, A) ->
                                 handoff_acc(K, HA, A, State)
                         end,
                         HandoffAccs, Accs),
    {ok, State#state{accs=NewAccs}}.

%% @doc The dict:merge function for handoff.  Handles the reducing.
-spec handoff_acc(term(), [term()], [term()], state()) -> [term()].
handoff_acc(Key, HandoffAccs, LocalAccs, State) ->
    InAcc = HandoffAccs++LocalAccs,
    case reduce(Key, InAcc, State) of
        {ok, OutAcc} ->
            OutAcc;
        {error, {Type, Error, Trace}} ->
                lager:error(
                  "~p:~p reducing handoff:~n   ~P~n   ~P",
                  [Type, Error, InAcc, 2, Trace, 5]),
            LocalAccs %% don't completely barf
    end.

%% @doc Actually evaluate the aggregation function.
-spec reduce(term(), [term()], state()) ->
         {ok, [term()]} | {error, {term(), term(), term()}}.
reduce(Key, InAcc, #state{p=Partition, fd=FittingDetails}) ->
    Fun = FittingDetails#fitting_details.arg,
    try
        {ok, OutAcc} = Fun(Key, InAcc, Partition, FittingDetails),
        true = is_list(OutAcc), %%TODO: nicer error
        {ok, OutAcc}
    catch Type:Error ->
            {error, {Type, Error, erlang:get_stacktrace()}}
    end.

%% @doc Check that the arg is a valid arity-4 function.  See {@link
%%      riak_pipe_v:validate_function/3}.
-spec validate_arg(term()) -> ok | {error, iolist()}.
validate_arg(Fun) when is_function(Fun) ->
    riak_pipe_v:validate_function("arg", 4, Fun);
validate_arg(Fun) ->
    {error, io_lib:format("~p requires a function as argument, not a ~p",
                          [?MODULE, riak_pipe_v:type_of(Fun)])}.

%% @doc The preferred hashing function.  Chooses a partition based
%%      on the hash of the `Key'.
-spec chashfun({term(), term()}) -> riak_pipe_vnode:chash().
chashfun({Key,_}) ->
    chash:key_of(Key).

%% @doc Serialize the archive for storage.
-spec serialize_archive(dict()) -> binary().
serialize_archive(Archive) ->
    term_to_binary(Archive).

%% @doc Deserialize the archive for storage.
-spec deserialize_archive(binary()) -> dict().
deserialize_archive(Archive) ->
    binary_to_term(Archive).

%% @doc Generate a key for the checkpoint.
%% @todo This may not be unique enough if they are running multiple of
%%       the riak_kv checkpointing reducers.
-spec checkpoint_key(state()) -> binary().
checkpoint_key(#state{p=Partition, fd=FittingDetails}) ->
    {_, {Module, _, _}} = process_info(self(), current_function),
    Name = FittingDetails#fitting_details.name,
    ListMod = atom_to_list(Module),
    ListName = atom_to_list(Name),
    list_to_binary(integer_to_list(Partition) ++
                   "_" ++ ListMod ++ "_" ++ ListName).

%% @doc Return the bucket used for checkpointing.
-spec checkpoint_bucket() -> binary().
checkpoint_bucket() ->
    app_helper:get_env(riak_kv,
                       vnode_checkpoint_bucket,
                       <<"__riak_kv_w_cp_reduce_checkpoints__">>).

%% @doc Generate initial state.
-spec initial_state() -> dict().
initial_state() ->
    dict:new().
