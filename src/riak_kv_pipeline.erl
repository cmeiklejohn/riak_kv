%% @author Christopher Meiklejohn <christopher.meiklejohn@gmail.com>
%% @copyright 2013 Christopher Meiklejohn.
%% @doc Pipeline.

-module(riak_kv_pipeline).
-author('Christopher Meiklejohn <christopher.meiklejohn@gmail.com>').

-behaviour(gen_server).

%% API
-export([start_link/2,
         accept/2,
         generate_listener/1,
         generate_unlistener/1,
         terminate/1,
         retrieve/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {name, pipe, fitting_specs}).

-include_lib("riak_pipe/include/riak_pipe.hrl").

-define(SERVER, ?MODULE).

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
%%--------------------------------------------------------------------
start_link(Name, FittingSpecs) ->
    gen_server:start_link({global, Name}, ?MODULE, [Name, FittingSpecs], []).

%% @doc Terminate a pipeline.
-spec terminate(atom()) -> ok | {error, unregistered}.
terminate(Name) ->
    case retrieve(Name) of
        undefined ->
            {error, unregistered};
        _Pid ->
            gen_server:call({global, Name}, terminate, infinity)
    end.

%% @doc Ingest a message into a pipeline.
-spec accept(atom(), term()) ->
    ok |
    {error, riak_pipe_vnode:qerror()} |
    {error, unregistered} |
    {error, retry}.
accept(Name, Message) ->
    case retrieve(Name) of
        undefined ->
            {error, unregistered};
        _Pid ->
            gen_server:call({global, Name}, {accept, Message}, infinity)
    end.

%% @doc Retrieve the pid of a pipeline's coordinating gen_server.
-spec retrieve(atom()) -> pid() | undefined.
retrieve(Name) ->
    global:whereis_name(Name).

%% @doc Generate listener.
-spec generate_listener(atom()) -> fun(() -> 'error' | 'ok').
generate_listener(Name) ->
    fun() ->
        case gproc:reg({p, g, Name}) of
            true ->
                ok;
            _ ->
                error
        end
    end.

%% @doc Generate unlistener.
-spec generate_unlistener(atom()) -> fun(() -> 'error' | 'ok').
generate_unlistener(Name) ->
    fun() ->
        case gproc:unreg(Name) of
            true ->
                ok;
            _ ->
                error
        end
    end.

%%====================================================================
%% gen_server callbacks
%%====================================================================

%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
%%--------------------------------------------------------------------
init([Name, FittingSpecs]) ->
    case initialize_pipeline(Name, FittingSpecs) of
        {ok, Pipe} ->
            {ok, #state{name=Name,
                        pipe=Pipe,
                        fitting_specs=FittingSpecs}};
        {error, Error} ->
            {stop, Error}
    end.

%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------
handle_call({accept, Message}, _From,
            #state{name=Name,
                   pipe=Pipe,
                   fitting_specs=FittingSpecs} = State) ->
    try
        Reply = riak_pipe:queue_work(Pipe, Message),
        {reply, Reply, State}
    catch
        _:_ ->
            case initialize_pipeline(Name, FittingSpecs) of
                {ok, Pipe} ->
                    NewState = #state{name=Name,
                                      pipe=Pipe,
                                      fitting_specs=FittingSpecs},
                    {reply, {error, retry}, NewState};
                {error, Error} ->
                    {reply, {error, Error}, State}
            end
    end;
handle_call(terminate, _From, State) ->
    Reply = ok,
    {stop, terminate, Reply, State};
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%%--------------------------------------------------------------------
handle_info(#pipe_result{} = PipeResult, State)->
    Name = State#state.name,
    Result = PipeResult#pipe_result.result,

    %% Broadcast to all members of the named process group.
    Result = gproc:send(Name, Result),

    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%%--------------------------------------------------------------------
terminate(_Reason, State) ->
    try
        Pipe = State#state.pipe,
        ok = riak_pipe:eoi(Pipe)
    catch
        _:_ ->
            ok
    end.

%%--------------------------------------------------------------------
%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------

%% @doc Key for fitting specifications for a pipeline.
-spec fittings_key(atom()) -> binary().
fittings_key(Name) ->
    list_to_binary("pipeline_" ++ atom_to_list(Name)).

%% @doc Bucket where to store the fitting specifications.
-spec fittings_bucket() -> binary().
fittings_bucket() ->
    app_helper:get_env(riak_kv,
                       vnode_fitting_bucket,
                       <<"__riak_kv_pipeline_fittings__">>).

%% @doc How to serialize fittings.
-spec serialize_fittings(list(#fitting_spec{})) -> binary().
serialize_fittings(FittingSpecs) ->
    term_to_binary(FittingSpecs).

%% @doc Store fitting speifications.
-spec store_fittings(atom(), list(#fitting_spec{})) -> ok.
store_fittings(Name, FittingSpecs) ->
    Key = fittings_key(Name),
    Bucket = fittings_bucket(),
    Value = serialize_fittings(FittingSpecs),
    Object = riak_object:new(Bucket, Key, Value),

    case riak:local_client() of
        {ok, Client} ->
            case Client:put(Object) of
                ok ->
                    lager:warning("Fitting storage successful.\n");
                PutError ->
                    lager:warning("Fitting storage failed. ~p\n",
                                  [PutError])
            end;
        ClientError ->
            lager:warning("Fitting storage: no riak client available. ~p\n",
                          [ClientError]),
            ClientError
    end,
    ok.

%% @doc Initialize pipeline.
-spec initialize_pipeline(atom(), list(#fitting_spec{})) ->
    {ok, term()} | {error, term()}.
initialize_pipeline(Name, FittingSpecs) ->
    case riak_pipe:exec(FittingSpecs, [{log, lager}]) of
        {ok, Pipe} ->
            ok = store_fittings(Name, FittingSpecs),
            {ok, Pipe};
        _ ->
            {error, failed_registration}
    end.

