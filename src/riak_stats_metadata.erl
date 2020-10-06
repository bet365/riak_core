%%%-----------------------------------------------------------------------------
%%% @doc
%%% Persistent configuration of the stats, in relation to it's use in
%%% exometer_core. Since the metrics have specific, sometimes constant options
%%% used exometer_core; this module parallels the use of these stats, but keeps
%%% each one's options to be re-used. i.e. the status of the stat, whether it is
%%% enabled or disabled, is stored in the cluster-metadata, and upon restarting
%%% the node the stats will remain enabled - disabled, instead of restarting as
%%% enabled by default.
%%% @end
%%%-----------------------------------------------------------------------------
-module(riak_stats_metadata).
-include("riak_stats.hrl").
-include("riak_stats_push.hrl").

-export([
    enabled/0,          reload_metadata/0,
    find_entries/3,     check_meta/1,
    put/3,              delete/2,
    get/2,              register/1,
    get_all/1,          change_status/1,
    get_all_stats/0,    set_options/2,
    unregister/1
]).

-define(STAT,                   stats).
-define(STAT_PREFIX,           {?STAT, node()}).
-define(STAT_KEY(StatName),    {?STAT_PREFIX, StatName}).
-define(STAT_MAP,               #{status  => enabled,
                                  type    => undefined,
                                  options => [],
                                  aliases => []}).

-type persist_prefix()     :: {binary() | atom(), binary() | atom()}.
-type persist_key()        :: any().
-type persist_prefix_key() :: {persist_prefix(), persist_key()}.

-type stat_value()         :: #{status  => status(),
                                type    => type(),
                                options => options(),
                                aliases => aliases()} |
                                {status(),type(),options(),aliases()}.
-type meta_value()         :: stat_value() | push_value() | list() | function().

%%%=============================================================================
%%% Main API
%%%=============================================================================
%%%-----------------------------------------------------------------------------
%% @doc Check the apps env for the status of the metadata @end
%%%-----------------------------------------------------------------------------
-spec(enabled() -> enabled | disabled).
enabled() ->
    app_helper:get_env(?PERSIST_APP, ?PERSIST_ENV, enabled).

%%%-----------------------------------------------------------------------------
%% @doc
%% reload the cluster_metadata after it has been disabled, to match the current
%% configuration of status status' in exometer_core
%% @end
%%%-----------------------------------------------------------------------------
-spec(reload_metadata() -> no_return()).
reload_metadata() ->
    {Stats,_} = riak_stats:find_entries([['_']],'_'),
    [change_status(Stat, Status) || {Stat, _Type, Status} <- Stats].

%%%-----------------------------------------------------------------------------
%% @doc
%% Use cluster_metadata:fold/4 to fold over the path in the metadata and pull
%% out the stats that match the STATUS and TYPE.
%% @end
%%%-----------------------------------------------------------------------------
-spec(find_entries(stats(),status(),type()) -> found_stats()).
find_entries(Stats,Status,Type) ->
    %% This isn't what it looks like, it is not taking every single stat out,
    %% when the data is sanitised for "stat.**.name", it creates a list within
    %% a list such as: [[stat,'_',name],[stat,'_','_',name]...] for each one
    %% of these we need to do a fold. @see stats_console:sanitise_stat_input/1
    lists:foldl(fun(S,Acc) -> fold_stat(S, Status, Type) ++ Acc end,[], Stats).

%%%-----------------------------------------------------------------------------
%% @doc
%% Pass the StatName into the fold, to match ({match,Stat}) the objects
%% stored in the metadata, under the Prefix: @see :  ?STAT_PREFIX
%%%-----------------------------------------------------------------------------
-spec(fold_stat(stats(),status(),type()) -> found_stats()).
%%%-----------------------------------------------------------------------------
%% Returns the same as exometer:find_entries ->
%%          [{Name,Type,Status}|...]
%% @end
%%%-----------------------------------------------------------------------------
fold_stat(Stat, Status0, Type0) ->
    {Stats, _, _} =
        riak_core_metadata:fold(
            fun({Name, [#{status := MStatus, type := MType}]},{Acc,Status,Type})
                when    (Status == MStatus  orelse Status == '_')
                andalso (MType  == Type     orelse Type   == '_')->
                {[{Name, MType, MStatus}|Acc],Status,Type};

                (_Other, {Acc, Status, Type}) ->
                    {Acc, Status, Type}
            end,
            {[], Status0, Type0}, ?STAT_PREFIX, [{match, Stat}]),
    Stats.

%%%-----------------------------------------------------------------------------
%% @doc Checks the metadata for the pkey provided @end
%%%-----------------------------------------------------------------------------
-spec(check_meta(stat_name() | persist_prefix_key()) -> meta_value()).
check_meta({Prefix, Key}) ->
    case get(Prefix, Key) of
        undefined -> % Not found, return empty list
            [];
        Value ->
            case find_unregister_status(Value) of
                false        -> Value;
                unregistered -> unregistered;
                Other        -> Other
            end
    end.

find_unregister_status('$deleted')                -> unregistered;
find_unregister_status(#{status := unregistered}) -> unregistered;
find_unregister_status(Map) when is_map(Map)      -> Map;
find_unregister_status(_)                         -> false.
%% if the value is anything but '$deleted' or unregistered, it returns
%% false -> i.e. it is not unregistered/deleted.

%% deleted is profile specific, unregistered is metric specific.

%%%=============================================================================
%%% Basic API
%%%=============================================================================
%%%-----------------------------------------------------------------------------
%% @doc Put into the metadata @end
%%%-----------------------------------------------------------------------------
-spec(put(persist_prefix(), persist_key(), meta_value()) -> ok).
put(Prefix, Key, Value) ->
    riak_core_metadata:put(Prefix, Key, Value).

%%%-----------------------------------------------------------------------------
%% @doc Pulls out information from cluster_metadata @end
%%%-----------------------------------------------------------------------------
-spec(get(persist_prefix(), persist_key()) -> meta_value() | undefined).
get(Prefix, Key) ->
    riak_core_metadata:get(Prefix, Key).

%%%-----------------------------------------------------------------------------
%% @doc
%% Give a Prefix for anything in the metadata and get a list of all the
%% data stored under that prefix
%% @end
%%%-----------------------------------------------------------------------------
-spec(get_all(persist_prefix()) -> [persist_prefix_key()]).
get_all(Prefix) ->
    riak_core_metadata:to_list(Prefix).

get_all_stats() ->
    get_all(?STAT_PREFIX).

%%%-----------------------------------------------------------------------------
%% @doc deleting the key from the metadata replaces values with tombstone @end
%%%-----------------------------------------------------------------------------
-spec(delete(persist_prefix(), persist_key()) -> ok).
delete(Prefix, Key) ->
    riak_core_metadata:delete(Prefix, Key).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% Stats API %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%-----------------------------------------------------------------------------
%% @doc
%% Checks if the stat is already registered in the metadata, if not it registers
%% it, and pulls out the options and sends it back to go into exometer_core
%% @end
%%%-----------------------------------------------------------------------------
-spec(register(stat_name(),type(),options(),aliases()) -> [] | options()).
register({StatName, Type, Opts, Aliases}) ->
    register(StatName, Type, Opts, Aliases).
register(StatName, Type, Opts, Aliases) ->
    case check_meta(?STAT_KEY(StatName)) of
        unregistered -> [];

        [] -> re_register(StatName,{enabled,Type,Opts,Aliases}),
            Opts;

        MapValue = #{status := MStatus} ->
            {Status,NewOpts} =
                case proplists:get_value(status, Opts) of
                    undefined -> {MStatus,[{status, MStatus}|Opts]};
                    _ ->
                        Opts2=lists:keyreplace(status,1,Opts,{status,MStatus}),
                        {MStatus, Opts2}
                end,
            re_register(StatName,MapValue#{status=>Status, options => NewOpts}),
            NewOpts;

        _ -> lager:debug(
            "Could not register stat:~n{~p,[{~p,~p,~p,~p}]}~n",
            [StatName,undefined,Type,Opts,Aliases]),
            []
    end.

-spec(re_register(stat_name(), stat_value()) -> ok).
re_register(StatName,{Status,Type,Options,Aliases}) ->
    StatMap = ?STAT_MAP,
    Value = StatMap#{status  => Status,
                     type    => Type,
                     options => Options,
                     aliases => Aliases},
    re_register(StatName,Value);
re_register(StatName, Value) ->
    put(?STAT_PREFIX, StatName, Value).

%%%-----------------------------------------------------------------------------
%% @doc Changes the status of stats in the metadata @end
%%%-----------------------------------------------------------------------------
-spec(change_status(stat_name(), status()) -> [] | {stat_name(),status()}).
change_status({StatName, Status}) -> change_status(StatName, Status);
change_status(Stats) ->
    [change_status(Stat, Status) || {Stat, Status} <- Stats].
change_status(Statname, ToStatus) ->
    case check_meta(?STAT_KEY(Statname)) of
        []           -> []; %% doesn't exist
        unregistered -> []; %% unregistered
        MapValue ->
            put(?STAT_PREFIX, Statname, MapValue#{status => ToStatus}),
            {Statname, ToStatus}
    end.

%%%-----------------------------------------------------------------------------
%% @doc Setting the options in the metadata @end
%%%-----------------------------------------------------------------------------
-type stat_info()        :: {stat_name(), stat_value()}.
-spec(set_options(stat_info(), options() | {atom(), any()}) -> ok).
set_options(StatInfo, NewOpts) when is_list(NewOpts) ->
    lists:foreach(fun({Key, NewVal}) ->
        set_options(StatInfo, {Key, NewVal})
                  end, NewOpts);
set_options({StatName, {Status, Type, Opts, Aliases}}, {Key, NewVal}) ->
    NewOpts = lists:keyreplace(Key, 1, Opts, {Key, NewVal}),
    re_register(StatName, {Status, Type, NewOpts, Aliases}).


%%%-----------------------------------------------------------------------------
%% @doc
%% Marks the stats as unregistered, that way when a node is restarted and
%% registers the stats it will be ignored
%% @end
%%%-----------------------------------------------------------------------------
-spec(unregister(stat_name()) -> ok).
unregister(Statname) ->
    case check_meta(?STAT_KEY(Statname)) of
        MapValue = #{status := Status} when Status =/= unregistered ->
            %% Stat exists, re-register with unregistered - "status"
            put(Statname,MapValue#{status => unregistered});
        _ -> ok
    end.