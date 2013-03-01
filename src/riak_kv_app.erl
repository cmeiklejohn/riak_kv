%% -------------------------------------------------------------------
%%
%% riak_app: application startup for Riak
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc Bootstrapping the Riak application.

-module(riak_kv_app).

-behaviour(application).
-export([start/2, prep_stop/1, stop/1]).
-export([check_kv_health/1]).

-define(SERVICES, [{riak_kv_pb_object, 3, 6}, %% ClientID stuff
                   {riak_kv_pb_object, 9, 14}, %% Object requests
                   {riak_kv_pb_bucket, 15, 22}, %% Bucket requests
                   {riak_kv_pb_mapred, 23, 24}, %% MapReduce requests
                   {riak_kv_pb_index, 25, 26} %% Secondary index requests
                  ]).
-define(MAX_FLUSH_PUT_FSM_RETRIES, 10).

%% @spec start(Type :: term(), StartArgs :: term()) ->
%%          {ok,Pid} | ignore | {error,Error}
%% @doc The application:start callback for riak.
%%      Arguments are ignored as all configuration is done via the erlenv file.
start(_Type, _StartArgs) ->
    riak_core_util:start_app_deps(riak_kv),

    %% Look at the epoch and generating an error message if it doesn't match up
    %% to our expectations
    check_epoch(),

    %% Append user-provided code paths
    case app_helper:get_env(riak_kv, add_paths) of
        List when is_list(List) ->
            ok = code:add_paths(List);
        _ ->
            ok
    end,

    %% Append defaults for riak_kv buckets to the bucket defaults
    %% TODO: Need to revisit this. Buckets are typically created
    %% by a specific entity; seems lame to append a bunch of unused
    %% metadata to buckets that may not be appropriate for the bucket.
    riak_core_bucket:append_bucket_defaults(
      [{linkfun, {modfun, riak_kv_wm_link_walker, mapreduce_linkfun}},
       {old_vclock, 86400},
       {young_vclock, 20},
       {big_vclock, 50},
       {small_vclock, 50},
       {pr, 0},
       {r, quorum},
       {w, quorum},
       {pw, 0},
       {dw, quorum},
       {rw, quorum},
       {basic_quorum, false},
       {notfound_ok, true}
   ]),

    %% Check the storage backend
    StorageBackend = app_helper:get_env(riak_kv, storage_backend),
    case code:ensure_loaded(StorageBackend) of
        {error,nofile} ->
            lager:critical("storage_backend ~p is non-loadable.",
                           [StorageBackend]),
            throw({error, invalid_storage_backend});
        _ ->
            ok
    end,

    %% Register our cluster_info app callback modules, with catch if
    %% the app is missing or packaging is broken.
    catch cluster_info:register_app(riak_kv_cinfo),

    %% Spin up supervisor
    case riak_kv_sup:start_link() of
        {ok, Pid} ->
            %% Register capabilities
            riak_core_capability:register({riak_kv, vnode_vclocks},
                                          [true, false],
                                          false,
                                          {riak_kv,
                                           vnode_vclocks,
                                           [{true, true}, {false, false}]}),

            riak_core_capability:register({riak_kv, legacy_keylisting},
                                          [false],
                                          false,
                                          {riak_kv,
                                           legacy_keylisting,
                                           [{false, false}]}),

            riak_core_capability:register({riak_kv, listkeys_backpressure},
                                          [true, false],
                                          false,
                                          {riak_kv,
                                           listkeys_backpressure,
                                           [{true, true}, {false, false}]}),

            riak_core_capability:register({riak_kv, index_backpressure},
                                          [true, false],
                                          false),

            %% mapred_system should remain until no nodes still exist
            %% that would propose 'legacy' as the default choice
            riak_core_capability:register({riak_kv, mapred_system},
                                          [pipe],
                                          pipe,
                                          {riak_kv,
                                           mapred_system,
                                           [{pipe, pipe}]}),

            riak_core_capability:register({riak_kv, mapred_2i_pipe},
                                          [true, false],
                                          false,
                                          {riak_kv,
                                           mapred_2i_pipe,
                                           [{true, true}, {false, false}]}),

            riak_core_capability:register({riak_kv, anti_entropy},
                                          [enabled_v1, disabled],
                                          disabled),

            %% Go ahead and mark the riak_kv service as up in the node watcher.
            %% The riak_core_ring_handler blocks until all vnodes have been started
            %% synchronously.
            riak_core:register(riak_kv, [
                {vnode_module, riak_kv_vnode},
                {health_check, {?MODULE, check_kv_health, []}},
                {bucket_validator, riak_kv_bucket},
                {stat_mod, riak_kv_stat}
            ]),

            ok = riak_api_pb_service:register(?SERVICES),

            %% Add routes to webmachine
            [ webmachine_router:add_route(R)
              || R <- lists:reverse(riak_kv_web:dispatch_table()) ],
            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end,
    document_environment().

%% @doc Prepare to stop - called before the supervisor tree is shutdown
prep_stop(_State) ->
    try %% wrap with a try/catch - application carries on regardless,
        %% no error message or logging about the failure otherwise.

        lager:info("Stopping application riak_kv - marked service down.\n", []),
        riak_core_node_watcher:service_down(riak_kv),

        ok = riak_api_pb_service:deregister(?SERVICES),
        lager:info("Unregistered pb services"),

        %% Gracefully unregister riak_kv webmachine endpoints.
        [ webmachine_router:remove_route(R) || R <-
            riak_kv_web:dispatch_table() ],
        lager:info("unregistered webmachine routes"),
        wait_for_put_fsms(),
        lager:info("all active put FSMs completed"),
        ok
    catch
        Type:Reason ->
            lager:error("Stopping application riak_api - ~p:~p.\n", [Type, Reason])
    end,
    stopping.

%% @spec stop(State :: term()) -> ok
%% @doc The application:stop callback for riak.
stop(_State) ->
    lager:info("Stopped  application riak_kv.\n", []),
    ok.

%% 719528 days from Jan 1, 0 to Jan 1, 1970
%%  *86400 seconds/day
-define(SEC_TO_EPOCH, 62167219200).

%% @spec check_epoch() -> ok
%% @doc
check_epoch() ->
    %% doc for erlang:now/0 says return value is platform-dependent
    %% -> let's emit an error if this platform doesn't think the epoch
    %%    is Jan 1, 1970
    {MSec, Sec, _} = os:timestamp(),
    GSec = calendar:datetime_to_gregorian_seconds(
             calendar:universal_time()),
    case GSec - ((MSec*1000000)+Sec) of
        N when (N < ?SEC_TO_EPOCH+5 andalso N > ?SEC_TO_EPOCH-5);
               (N < -?SEC_TO_EPOCH+5 andalso N > -?SEC_TO_EPOCH-5) ->
            %% if epoch is within 10 sec of expected, accept it
            ok;
        N ->
            Epoch = calendar:gregorian_seconds_to_datetime(N),
            lager:error("Riak expects your system's epoch to be Jan 1, 1970,"
                        "but your system says the epoch is ~p", [Epoch]),
            ok
    end.

check_kv_health(_Pid) ->
    VNodes = riak_core_vnode_manager:all_index_pid(riak_kv_vnode),
    {Low, High} = app_helper:get_env(riak_kv, vnode_mailbox_limit, {1, 5000}),
    case lists:member(riak_kv, riak_core_node_watcher:services(node())) of
        true ->
            %% Service active, use high watermark
            Mode = enabled,
            Threshold = High;
        false ->
            %% Service disabled, use low watermark
            Mode = disabled,
            Threshold = Low
    end,

    SlowVNs =
        [{Idx,Len} || {Idx, Pid} <- VNodes,
                      Info <- [process_info(Pid, [message_queue_len])],
                      is_list(Info),
                      {message_queue_len, Len} <- Info,
                      Len > Threshold],
    Passed = (SlowVNs =:= []),

    case {Passed, Mode} of
        {false, enabled} ->
            lager:info("Disabling riak_kv due to large message queues. "
                       "Offending vnodes: ~p", [SlowVNs]);
        {true, disabled} ->
            lager:info("Re-enabling riak_kv after successful health check");
        _ ->
            ok
    end,
    Passed.

wait_for_put_fsms(N) ->
    case riak_kv_get_put_monitor:puts_active() of
        0 -> ok;
        Count ->
            case N of
                0 ->
                    lager:warning("Timed out waiting for put FSMs to flush"),
                    ok;
                _ -> lager:info("Waiting for ~p put FSMs to complete",
                                [Count]),
                     timer:sleep(1000),
                     wait_for_put_fsms(N-1)
            end
    end.

wait_for_put_fsms() ->
    wait_for_put_fsms(?MAX_FLUSH_PUT_FSM_RETRIES).


-define(LINUX_PARAMS, [
                       {"vm.swappiness",                        0, gte}, 
                       {"net.core.wmem_default",          8388608, lte},
                       {"net.core.rmem_default",          8388608, lte},
                       {"net.core.wmem_max",              8388608, lte},
                       {"net.core.rmem_max",              8388608, lte},
                       {"net.core.netdev_max_backlog",      10000, lte},
                       {"net.core.somaxconn",                4000, lte},
                       {"net.ipv4.tcp_max_syn_backlog",     40000, lte},
                       {"net.ipv4.tcp_fin_timeout",            15, gte},
                       {"net.ipv4.tcp_tw_reuse",                1, eq}
                      ]).

%% @private
document_environment() ->
    lager:info("Environment and OS variables:"),
    lager:info("-----------------------------"),
    check_ulimits(),
    check_erlang_limits(),
    case os:type() of
        {unix, linux}  ->
             check_sysctls(?LINUX_PARAMS);
        {unix, freebsd} ->
            ok;
        {unix, sunos} ->
            ok;
        _ -> 
            lager:error("Unknown OS type, no platform specific info")
    end,
    lager:info("End environment dump.").

%% we don't really care about anything other than cores and open files
check_ulimits() ->
    %% file ulimit
    FileLimit0 = string:strip(os:cmd("ulimit -n"), right, $\n),
    case FileLimit0 of 
        "unlimited" -> 
            %% check the OS limit;
            OSLimit = case os:type() of 
                          {unix, linux} ->
                              string:strip(os:cmd("sysctl -n fs.file-max"), 
                                           right, $\n);
                          _ -> unknown
                      end,
            case OSLimit of
                unknown -> 
                    lager:warn("Open file limit unlimited but actual limit "
                               "could not be ascertained");
                _ -> 
                    test_file_limit(OSLimit)
            end;
        _ -> 
            test_file_limit(FileLimit0)
    end, 
    CoreLimit0 = string:strip(os:cmd("ulimit -n"), right, $\n),
    case CoreLimit0 of 
        "unlimited" -> 
            lager:info("No Core limit");
        _  ->
            CoreLimit = list_to_integer(CoreLimit0),
            case CoreLimit == 0 of 
                true ->
                    lager:warn("Cores are disabled, this may "
                               "hinder debugging");
                false ->
                    lager:info("Core limit: ~d", [CoreLimit])
            end
    end.

%% @private
test_file_limit(FileLimit0) ->
    FileLimit = (catch list_to_integer(FileLimit0)),
    case FileLimit of
        {'EXIT', {badarg,_}} -> 
            lager:warn("Open file limit was read as non-integer string: ~s",
                       [FileLimit0]);
    
        _ -> 
            case FileLimit =< 4096 of 
                true ->
                    lager:warn("Open file limit of ~d is low, at least "
                               "4096 is recommended", [FileLimit]);
                false -> 
                    lager:info("Open file limit: ~d", [FileLimit])
            end
    end.      

check_erlang_limits() ->
    %% processes
    case erlang:system_info(process_limit) of
        PL1 when PL1 < 4096 ->
            lager:warn("Process limit of ~d is low, at least "
                       "4096 is recommended", [PL1]);
        PL2 ->
            lager:info("Open file limit: ~d", [PL2])
    end,
    %% ports
    PortLimit = case os:getenv("ERL_MAX_PORTS") of
                    false -> 1024;
                    PL -> list_to_integer(PL)
                end,
    case PortLimit < 4096 of
        true ->
            %% needs to be revisited for R16+
            lager:warn("Erlang ports limit of ~d is low, at least "
                       "4096 is recommended", [PortLimit]);
        false ->
            lager:info("Erlang ports limit: ~d", [PortLimit])
    end,
        
    %% ets tables
    ETSLimit = case os:getenv("ERL_MAX_ETS_TABLES") of
                   false -> 1400;
                   Limit -> list_to_integer(Limit)
               end,
    case ETSLimit < 8192 of
        true ->
            lager:warn("ETS table count limit of ~d is low, at least "
                       "8192 is recommended.", [ETSLimit]);
        false ->
            larger:info("ETS table count limit: ~d",
                        [ETSLimit])
    end,
    %% fullsweep_after
    GCGens = erlang:system_info(fullsweep_after),
    lager:info("Fullsweep after setting: ~d", [GCGens]),
    %% async_threads
    case erlang:system_info(thread_pool_size) of
        TPS1 when TPS1 < 64 ->
            lager:warn("Thread pool size of ~d is low, at least 64 suggested",
                       [TPS1]);
        TPS2 ->
            lager:info("Thread pool size: ~d", [TPS2])
    end,
    %% schedulers
    Schedulers = erlang:system_info(schedulers),
    Cores = erlang:system_info(logical_processors_available),
    case Schedulers /= Cores of
        true ->
            lager:warn("Running ~d schedulers for ~d cores, "
                       "these should match",
                      [Schedulers, Cores]);
        false ->
            lager:info("Schedulers: ~d for ~d cores", 
                       [Schedulers, Cores])
    end.

check_sysctls(Checklist) ->
    Fn = fun({Param, Val, Direction}) ->
                 Output = string:strip(os:cmd("sysctl -n"++Param), right, $\n),
                 Actual = list_to_integer(Output -- "\n"),
                 Good = case Direction of
                            gte -> Actual =< Val;
                            lte -> Actual >= Val;
                            eq -> Actual == Val
                        end,
                 case Good of 
                     true ->
                         lager:info("sysctl ~s is ~p ~s ~p)", 
                                    [Param, Actual, 
                                     direction_to_word(Direction), 
                                     Val]);
                     false -> 
                         lager:error("sysctl ~s is ~p, should be ~s~p)", 
                                     [Param, Actual, 
                                      direction_to_word2(Direction), 
                                      Val])
                 end
         end,
    lists:map(Fn, Checklist).
                 
direction_to_word(Direction) ->
    case Direction of 
        gte -> "greater than or equal to";
        lte -> "lesser than or equal to";
        eq  -> "equal to"
    end.

direction_to_word2(Direction) ->
    case Direction of 
        gte -> "no more than ";
        lte -> "at least ";
        eq  -> ""
    end.

                 
               

