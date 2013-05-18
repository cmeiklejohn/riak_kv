%% @author Christopher Meiklejohn <christopher.meiklejohn@gmail.com>
%% @copyright 2013 Christopher Meiklejohn.
%% @doc Resource for managing individual pipelines.

-module(riak_kv_wm_pipeline).
-author('Christopher Meiklejohn <christopher.meiklejohn@gmail.com').

-export([init/1,
         to_stream/2,
         from_stream/2,
         process_post/2,
         delete_resource/2,
         allowed_methods/2,
         resource_exists/2,
         content_types_provided/2,
         content_types_accepted/2]).

-include_lib("riak_pipe/include/riak_pipe.hrl").
-include_lib("webmachine/include/webmachine.hrl").

-record(context, {pipeline}).

%% @doc Initialize the resource.
init(_Config) ->
    {ok, #context{pipeline=undefined}}.

%% @doc Support sending and receiving of events.
allowed_methods(ReqData, Context) ->
    {['HEAD', 'GET', 'POST', 'PUT', 'DELETE'], ReqData, Context}.

%% @doc Resource exists if the pipeline is registered.
resource_exists(ReqData, Context) ->
    Name = wrq:path_info(pipeline, ReqData),

    try
        Pipeline = list_to_existing_atom(Name),

        case riak_kv_pipeline:retrieve(Pipeline) of
            undefined ->
                {false, ReqData, Context};
            _Pid ->
                {true, ReqData, #context{pipeline=Pipeline}}
        end
    catch
        error:badarg ->
            {false, ReqData, Context}
    end.

%% @doc Delete a pipeline.
delete_resource(ReqData, Context) ->
    Pipeline = Context#context.pipeline,

    Response = case riak_kv_pipeline:terminate(Pipeline) of
        ok ->
            true;
        {error, _Error} ->
            false
    end,

    {Response, ReqData, Context}.

%% @doc Provide data in byte streams only.
content_types_provided(ReqData, Context) ->
    {[{"application/octet-stream", to_stream}], ReqData, Context}.

%% @doc Accept data in byte streams only.
content_types_accepted(ReqData, Context) ->
    {[{"application/octet-stream", from_stream}], ReqData, Context}.

%% @doc Ingest messages.
from_stream(ReqData, Context) ->
    Body = wrq:req_body(ReqData),
    Pipeline = Context#context.pipeline,

    case riak_kv_pipeline:accept(Pipeline, Body) of
        ok ->
            lager:warning("Successful event ingestion: ~p\n",
                          [Pipeline]),
            {true, ReqData, Context};
        {error, unregistered} ->
            lager:warning("Failed pipeline unregistered: ~p\n",
                          [Pipeline]),
            {{halt, 404}, ReqData, Context};
        {error, Error} ->
            lager:warning("Failed event ingestion: ~p ~p\n",
                          [Pipeline, Error]),
            {{halt, 503}, ReqData, Context}
    end.

%% @doc Ingest messages.
process_post(ReqData, Context) ->
    Id = erlang:phash2(now()),
    Body = wrq:req_body(ReqData),
    Pipeline = Context#context.pipeline,

    case listen(Pipeline) of
        ok ->
            case riak_kv_pipeline:accept(Pipeline, {Id, Body}) of
                ok ->
                    receive
                        {Id, _} = Response ->
                            NewResponse = encode(Response),
                            NewReqData = wrq:set_resp_body(NewResponse, ReqData),
                            {true, NewReqData, Context}
                    end;
                {error, unregistered} ->
                    lager:warning("Failed pipeline unregistered: ~p\n",
                                  [Pipeline]),
                    {{halt, 404}, ReqData, Context};
                {error, Error} ->
                    lager:warning("Failed event ingestion: ~p ~p\n",
                                  [Pipeline, Error]),
                    {{halt, 503}, ReqData, Context}
            end;
        _ ->
            lager:warning("Failed listener: ~p ~p.\n", [Pipeline, self()]),
            {{halt, 500}, ReqData, Context}
    end.

%% @doc Stream messages from the pipeline.
to_stream(ReqData, Context) ->
    Pipeline = Context#context.pipeline,

    case listen(Pipeline) of
        ok ->
            Boundary = riak_core_util:unique_id_62(),
            NewReqData = wrq:set_resp_header("Content-Type",
                                             "multipart/mixed; boundary=" ++ Boundary,
                                             ReqData),
            {{stream, {<<>>, fun() -> stream(Boundary) end}}, NewReqData, Context};
        _ ->
            {{halt, 500}, ReqData, Context}
    end.

%% @doc Stream data from the pipeline out.
stream(Boundary) ->
    receive
        Content ->
            Body = ["\r\n--", Boundary,
                    "\r\nContent-Type: application/octet-stream",
                    "\r\n\r\n", encode(Content), "\r\n"],
            {Body, fun() -> stream(Boundary) end}
    end.

%% @doc Encode content.
encode(Content) ->
    term_to_binary(Content).

%% @doc Generate a listener and listen.
listen(Pipeline) ->
    Listener = riak_kv_pipeline:generate_listener(Pipeline),
    Listener().
