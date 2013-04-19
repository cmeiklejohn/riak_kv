%% @author Christopher Meiklejohn <christopher.meiklejohn@gmail.com>
%% @copyright 2013 Christopher Meiklejohn.
%% @doc Resource for managing individual pipelines.

-module(riak_kv_wm_pipeline).
-author('Christopher Meiklejohn <christopher.meiklejohn@gmail.com').

-export([init/1,
         to_stream/2,
         from_stream/2,
         process_post/2,
         finish_request/2,
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
    {['HEAD', 'GET', 'POST', 'PUT'], ReqData, Context}.

%% @doc Resource exists if the pipeline is registered.
resource_exists(ReqData, Context) ->
    Name = wrq:path_info(pipeline, ReqData),

    try
        Pipeline = list_to_existing_atom(Name),

        case riak_kv_pipeline:retrieve(Pipeline) of
            undefined ->
                {false, ReqData, Context};
            _ ->
                {true, ReqData, #context{pipeline=Pipeline}}
        end
    catch
        error:badarg ->
            {false, ReqData, Context}
    end.

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
            {true, ReqData, Context};
        {error, _} ->
            {{halt, 400}, ReqData, Context}
    end.

%% @doc Ingest messages.
process_post(ReqData, Context) ->
    Body = wrq:req_body(ReqData),
    Pipeline = Context#context.pipeline,

    case riak_kv_pipeline:accept(Pipeline, Body) of
        ok ->
            {true, ReqData, Context};
        {error, _} ->
            {{halt, 400}, ReqData, Context}
    end.

%% @doc Stream messages from the pipeline.
to_stream(ReqData, Context) ->
    Pipeline = Context#context.pipeline,

    case riak_kv_pipeline:listen(Pipeline, self()) of
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
                    "\r\n\r\n", Content, "\r\n"],
            {Body, fun() -> stream(Boundary) end}
    end.

%% @doc When we're finished, ensure we are not listening for events.
finish_request(ReqData, Context) ->
    Pipeline = Context#context.pipeline,

    %% Unlisten.
    _ = riak_kv_pipeline:unlisten(Pipeline, self()),
    {true, ReqData, Context}.