%% @author Christopher Meiklejohn <christopher.meiklejohn@gmail.com>
%% @copyright 2013 Christopher Meiklejohn.
%% @doc Resource for managing pipelines.

-module(riak_kv_wm_pipelines).
-author('Christopher Meiklejohn <christopher.meiklejohn@gmail.com').

-export([init/1,
         from_json/2,
         create_path/2,
         post_is_create/2,
         allowed_methods/2,
         content_types_accepted/2]).

-include_lib("riak_pipe/include/riak_pipe.hrl").
-include_lib("webmachine/include/webmachine.hrl").

-record(context, {pipeline}).

%% @doc Initialize the resource.
init(_Config) ->
    {ok, #context{pipeline=undefined}}.

%% @doc Support retrieval and creation.
allowed_methods(ReqData, Context) ->
    {['POST'], ReqData, Context}.

%% @doc POST is going to create a new resource.
post_is_create(ReqData, Context) ->
    {true, ReqData, Context}.

%% @doc If possible, create and return path.
create_path(ReqData, Context) ->
    PipelineProps = riak_kv_web:pipeline_props(),
    Root = "/" ++ proplists:get_value(prefix, PipelineProps),

    case maybe_create_pipeline(ReqData, Context) of
        {true, NewContext} ->
            case NewContext#context.pipeline of
                undefined ->
                    {Root, ReqData, NewContext};
                Pipeline ->
                    Resource = Root ++ "/" ++ atom_to_list(Pipeline),
                    NewReqData = wrq:set_resp_header("Location", Resource, ReqData),
                    {Root, NewReqData, NewContext}
            end;
        {false, Context} ->
            {Root, ReqData, Context}
    end.

%% @doc Accept data in JSON only.
content_types_accepted(ReqData, Context) ->
    {[{"application/json", from_json}], ReqData, Context}.

%% @doc Return 400 if we can't create.
%%
%% @todo Return 409 if the pipeline already exists.
from_json(ReqData, Context) ->
    case maybe_create_pipeline(ReqData, Context) of
        {true, NewContext} ->
            {true, ReqData, NewContext};
        {false, Context} ->
            {{halt, 400}, ReqData, Context}
    end.

%% @doc Attempt to create and stash in the context if possible.
maybe_create_pipeline(ReqData, Context) ->
    Body = wrq:req_body(ReqData),

    lager:info("Attempting pipeline registration..."),

    case Context#context.pipeline of
        undefined ->
            case Body of
                <<"">> ->
                    lager:info("Pipeline registration failed, no body."),
                    {false, Context};
                _ ->
                    lager:info("Pipeline registering..."),
                    RawPipeline = mochijson2:decode(Body),
                    {struct, AtomPipeline} = atomize(RawPipeline),
                    Name = atomized_get_value(name, AtomPipeline, undefined),
                    Fittings = proplists:get_value(fittings, AtomPipeline),
                    FittingSpecs = fittings_to_fitting_specs(Fittings),

                    case register_pipeline(Name, FittingSpecs) of
                        {ok, _} ->
                            {true, Context#context{pipeline=Name}};
                        {error, Error} ->
                            lager:info("Pipeline registration failed: ~p", [Error]),
                            {false, Context}
                    end
                end;
        _Pipeline ->
            {true, Context}
    end.

%% @doc Given a struct/proplist that we've received via JSON,
%% recursively turn the keys into atoms from binaries.
atomize({struct, L}) ->
    {struct, [{binary_to_existing_atom(I, utf8), atomize(J)} || {I, J} <- L]};
atomize(L) when is_list(L) ->
    [atomize(I) || I <- L];
atomize(X) ->
    X.

%% @doc Start a pipeline.
register_pipeline(Name, FittingSpecs) ->
    riak_kv_pipeline_sup:start_pipeline(Name, FittingSpecs).

%% @doc Convert fittings to fitting specs.
%%
%% @todo This method needs much better error checking; potentially
%%       returning an ok | error tuple type response.  Also, explore
%%       removing the atomize method and operating directly on the
%%       binaries.
fittings_to_fitting_specs(Fittings) ->
    lists:foldl(fun({struct, Fitting}, FittingSpecs) ->
                Name = atomized_get_value(name, Fitting, foo),
                Module = atomized_get_value(module, Fitting, riak_pipe_w_pass),
                Arg = case atomized_get_value(arg, Fitting, undefined) of
                    {struct, Args} ->
                        ArgModule = atomized_get_value(module, Args, undefined),
                        ArgFunction = atomized_get_value(function, Args, undefined),
                        ArgArity = atomized_get_value(arity, Args, 0),
                        erlang:make_fun(ArgModule, ArgFunction, ArgArity);
                    Value ->
                       Value
                end,
                FittingSpecs ++ [generate_fitting_spec(Name, Module, Arg)]
        end, [], Fittings).

%% @doc Generate a riak pipe fitting specification.
-spec generate_fitting_spec(atom(), atom(), term()) -> #fitting_spec{}.
generate_fitting_spec(Name, Module, Arg) ->
    #fitting_spec{name=Name, module=Module, arg=Arg}.

%% @doc
%%
%% Return a value from a proplist, and ensure it's an atom.
%%
%% Possible atom-table injection attack here, but necessary until we
%% adapt pipe to take things other than atoms.
%%
%% @end
atomized_get_value(Key, List, Default) ->
    Result = proplists:get_value(Key, List, Default),
    case is_binary(Result) of
        true ->
            binary_to_atom(Result, utf8);
        false ->
            Result
    end.
