%% @author Christopher Meiklejohn <christopher.meiklejohn@gmail.com>
%% @copyright 2013 Christopher Meiklejohn.
%% @doc Pipeline.

-module(riak_kv_pipeline).
-author('Christopher Meiklejohn <christopher.meiklejohn@gmail.com>').

-behaviour(gen_server).

%% API
-export([start_link/2,
         accept/2,
         listen/2,
         unlisten/2,
         terminate/1,
         retrieve/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {name, pipe}).

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
    ok | {error, riak_pipe_vnode:qerror()} | {error, unregistered}.
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

%% @doc Register a pid to receive results from the pipeline.
-spec listen(atom(), pid()) -> ok | {error, {no_such_group, atom()}}.
listen(Name, Pid) ->
    %% Attempt to create a process group for watching pipeline.
    ok = pg2:create(Name),

    %% Add myself to the process list.
    pg2:join(Name, Pid).

%% @doc Unregister a listener.
-spec unlisten(atom(), pid()) -> ok | {error, {no_such_group, atom()}}.
unlisten(Name, Pid) ->
    pg2:leave(Name, Pid).

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
    case riak_pipe:exec(FittingSpecs, [{log, lager}]) of
        {ok, Pipe} ->
            {ok, #state{name=Name, pipe=Pipe}};
        _ ->
            {stop, failed_registration}
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
handle_call({accept, Message}, _From, State) ->
    Pipe = State#state.pipe,
    Reply = riak_pipe:queue_work(Pipe, Message),
    {reply, Reply, State};
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
    _ = case pg2:get_members(Name) of
        {error, _} ->
            ok;
        Pids ->
            [Pid ! Result || Pid <- Pids]
    end,

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
    Pipe = State#state.pipe,
    riak_pipe:eoi(Pipe),
    ok.

%%--------------------------------------------------------------------
%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------
