%% @author Christopher Meiklejohn <christopher.meiklejohn@gmail.com>
%% @copyright 2013 Christopher Meiklejohn.
%% @doc Supervisor for the pipelines.

-module(riak_kv_pipeline_sup).
-author('Christopher Meiklejohn <christopher.meiklejohn@gmail.com>').

-behaviour(supervisor).

%% API
-export([start_link/0,
         start_pipeline/2]).

%% Supervisor callbacks
-export([init/1]).

%% ===================================================================
%% API functions
%% ===================================================================

%% @doc API for starting the supervisor.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% @doc Start a pipeline.
start_pipeline(Name, FittingSpecs) ->
    supervisor:start_child(?MODULE, [Name, FittingSpecs]).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

%% @doc supervisor callback.
init([]) ->
    PipelineSpec = {riak_kv_pipeline,
                    {riak_kv_pipeline, start_link, []},
                     temporary, 5000, worker, [riak_kv_pipeline]},

    {ok, {{simple_one_for_one, 10, 10}, [PipelineSpec]}}.
