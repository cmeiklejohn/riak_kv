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
                    NewReqData = add_location_header(Resource, ReqData),
                    {Root, NewReqData, NewContext}
            end;
        {false, Context} ->
            {Root, ReqData, Context}
    end.

%% @doc Generate a response object with a location header.
-spec add_location_header(list(), wrq:req_data()) -> wrq:req_data().
add_location_header(Resource, ReqData) ->
    wrq:set_resp_header("Location", Resource, ReqData).

%% @doc Accept data in JSON only.
content_types_accepted(ReqData, Context) ->
    {[{"application/json", from_json}], ReqData, Context}.

%% @doc Return 400 if we can't create.
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

    case Context#context.pipeline of
        undefined ->
            case Body of
                <<"">> ->
                    {false, Context};
                _ ->
                    {struct, DecodedBody} = mochijson2:decode(Body),

                    RawName = proplists:get_value(<<"name">>,
                                                  DecodedBody,
                                                  <<"undefined">>),
                    Name = binary_to_atom(RawName, utf8),

                    Fittings = proplists:get_value(<<"fittings">>,
                                                   DecodedBody,
                                                   []),

                    %% TODO: better error handling
                    {ok, Specs} = fittings_to_fitting_specs(Fittings),

                    case register_pipeline(Name, Specs) of
                        {ok, _Pid} ->
                            lager:warning("Pipeline started: ~p.\n",
                                          [Name]),
                            ok;
                        {error, already_started} ->
                            lager:warning("Pipeline already started: ~p.\n",
                                          [Name]),
                            ok;
                        {error, _Error} ->
                            lager:warning("Pipeline failed to start: ~p.\n",
                                          [Name]),
                            ok
                    end,

                    {true, Context#context{pipeline=Name}}
                end;
        _Pipeline ->
            {true, Context}
    end.

%% @doc Start a pipeline.
register_pipeline(Name, FittingSpecs) ->
    riak_kv_pipeline_sup:start_pipeline(Name, FittingSpecs).

%% @doc Convert fittings to fitting specs.
fittings_to_fitting_specs(Fittings) ->
    FittingSpecs = lists:foldl(fun({struct, Fitting}, FittingSpecs) ->
                RawName = proplists:get_value(<<"name">>,
                                              Fitting,
                                              <<"undefined">>),
                Name = binary_to_atom(RawName, utf8),

                RawModule = proplists:get_value(<<"module">>,
                                                Fitting,
                                                <<"riak_pipe_w_pass">>),
                Module = binary_to_atom(RawModule, utf8),

                RawArg = proplists:get_value(<<"arg">>, Fitting, <<"">>),

                Arg = case RawArg of
                    {struct, Args} ->
                        {Module1, Function1, Arity1} = args_to_mfa(Args),
                        make_fun(Module1, Function1, Arity1);
                    Value ->
                       Value
                end,
                FittingSpecs ++ [generate_fitting_spec(Name, Module, Arg)]
        end, [], Fittings),
    {ok, FittingSpecs}.

%% @doc Given a proplist, generate an MFA.
-spec args_to_mfa(list()) -> {term(), term(), term()}.
args_to_mfa(Args) ->
    Module = proplists:get_value(<<"module">>, Args),
    Function = proplists:get_value(<<"function">>, Args),
    Arity = proplists:get_value(<<"arity">>, Args),
    {Module, Function, Arity}.

%% @doc Generate a function from a MFA.
-spec make_fun(term(), term(), term()) -> function().
make_fun(Module, Function, Arity) ->
    erlang:make_fun(
        binary_to_atom(Module, utf8),
        binary_to_atom(Function, utf8),
        Arity).

%% @doc Generate a riak pipe fitting specification.
-spec generate_fitting_spec(atom(), atom(), term()) -> #fitting_spec{}.
generate_fitting_spec(Name, Module, Arg) ->
    #fitting_spec{name=Name, module=Module, arg=Arg}.
