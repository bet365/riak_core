%%%-----------------------------------------------------------------------------
%%% @doc
%%% Administration of stats in to be registered in exometer_core and store the
%%% configuration of the stats in the riak_core_metadata
%%% @end
%%%-----------------------------------------------------------------------------
-module(riak_stats).
-include("riak_stats.hrl").

%% API
%% API
-export([register/1, get_stats/1, find_entries/2, find_entries/4, stats/0,
         get_value/1, get_datapoint/2, get_values/1,
         update/3, unregister/1, change_status/1, reset/1,
         alias/1, aliases_prefix_foldl/0, timestamp/0]).

-define(CACHE, riak_stat_cache).

%%%=============================================================================
%%% API
%%%=============================================================================
%%%-----------------------------------------------------------------------------
%%% @doc
%%% For registration the stats call into this module to become an uniform format
%%% in a list to be "re-registered" in the metadata and in exometer. Unless the
%%% metadata is disabled (it is enabled by default), the stats are sent to both
%%% to be registered. The purpose of the metadata is to allow persistence of the
%%% stats registration and configuration
%%%         - whether the stat is enabled/disabled/unregistered;
%%% Exometer does not persist the stats information.
%%% Metadata does not persist the stats values.
%%% @end
%%%-----------------------------------------------------------------------------
-spec(register(input_stats()) -> ok).
register(Stats) ->
    lists:foreach(fun
          ({Name,Type})                -> register_stat(Name,Type,[],[]);
          ({Name,Type,Option})         -> register_stat(Name,Type,Option,[]);
          ({Name,Type,Option,Aliases}) -> register_stat(Name,Type,Option,Aliases)
                  end, Stats),
    ok.

register_stat(Name, Type, Options, Aliases) ->
    NewOptions = add_cache(Options),
    register_stats({Name, Type, NewOptions, Aliases}).

add_cache(Options) ->
    %% look for cache value in stat, if undefined - add cache option
    case proplists:get_value(cache, Options) of
        undefined ->
            Default = application:get_env(stats, ?CACHE, 5000),
            [{cache,Default} | Options];
        _ -> Options %% Otherwise return the Options as is
    end.

-spec(register_stats(tuple_stat()) -> ok).
register_stats({StatName, Type, Opts, Aliases}=StatTuple) ->
    case riak_stats_metadata:enabled() of
        false ->
            re_register(StatName,Type,Opts,Aliases);
        true ->
            NewOpts = riak_stats_metadata:register(StatTuple),
            re_register(StatName,Type,NewOpts,Aliases)
    end.

re_register(StatName, Type, Opts, Aliases) ->
    exometer:re_register(StatName, Type, Opts),
    lists:foreach(fun({DP,Alias}) ->
        exometer_alias:new(Alias,StatName,DP)
                  end, Aliases),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% NB : DP = DataPoint
%%      Alias : Name of a datapoint of a Stat.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%-----------------------------------------------------------------------------
%% @doc
%% Give a path to a stat such as : [stat,test,node,gets,time] to retrieve the
%% stats' as : [{Name, Type, Status}]
%% @end
%%%-----------------------------------------------------------------------------
-spec(get_stats(stat_path()) -> found_entries()).
get_stats(Path) ->
    {FoundStats,DataPoints} = find_entries(Path, '_'),
    {FoundStats,DataPoints}.

find_entries(Stats, Status) ->
    find_entries(Stats, Status, '_', []).

%%%-----------------------------------------------------------------------------
%%% @doc
%%% look for the stat in @see legacy_search first, if it is not the alias of a
%%% metric then check in the metadata, the metadata takes slightly longer than
%%% exometer to retrieve (0.01 millis) but it reduces the traffic to exometer,
%%% if the metadata is disabled it will look in exometer anyway, or if it cannot
%%% find it.
%%% @end
%%%-----------------------------------------------------------------------------
-spec(find_entries(stat_path(),status(),type(),datapoints()) -> found_entries()).
find_entries(Stats, Status, Type, DPs) ->
    %% check in legacy search first, as it takes less time to perform
    case legacy_search(Stats, Status, Type) of
        [] ->
            case riak_stats_metadata:enabled() of
                false -> try_find_in_mem(Stats, Status, Type, DPs);
                true -> try_find_in_meta(Stats, Status, Type, DPs)
            end;
        [NewStats] ->  NewStats; %% alias' search
        Otherwise -> Otherwise   %% legacy search
    end.

try_find_in_mem(Stats, Status, Type, DPs) ->
    MatchSpec = make_matchspec(Stats, Status, Type),
    case exometer:select(MatchSpec) of
        [] ->
            try find_mem_entries(Stats, Status) of
                NewStats ->
                    {NewStats, DPs}
            catch _:_ ->
                {[],[]}
            end;
        NewStats ->
            {NewStats, DPs}
    end.

make_matchspec(Stats,Status,Type) ->
    [{{Stat,Type,Status},[],['$_']} || Stat <- Stats].

find_mem_entries(Stats, Status) ->
    [lists:foldl(fun
                     ({Name, Type, EStatus}, Found)
                         when EStatus == Status orelse Status == '_' ->
                         [{Name, Type, Status} | Found];
                     (_, Found) -> Found % Different status
                 end,[], exometer:find_entries(Stat)) || Stat <- Stats].

try_find_in_meta(Stats, Status, Type, DPs) ->
    case riak_stats_metadata:find_entries(Stats, Status, Type) of
        [] -> %% unregistered
            try_find_in_mem(Stats, Status, Type, DPs);
        NewStats ->
            {NewStats, DPs}
    end.

%%%-----------------------------------------------------------------------------
%%% @doc
%%% legacy search looks for the alias in exometer_alias to return the stat name
%%% and its value.
%%% @end
%%%-----------------------------------------------------------------------------
-spec(legacy_search(stat_path(),status(),type()) -> found_stats()|legacy_stat()).
legacy_search(Stats, Status, Type) ->
    lists:flatten(
        lists:map(fun(Stat) ->
            legacy_search_(Stat, Status, Type)
                  end, Stats)).

legacy_search_(Stat, Status, Type) ->
    try re:run(Stat, "\\.",[]) of
        {match, _} -> %% wrong format, should not match
            [];
        nomatch ->
            Re = <<"^", (make_re(Stat))/binary, "$">>,
            [{Stat, legacy_search_cont(Re, Status, Type)}]
    catch _:_ ->
        find_through_alias(Stat)
    end.

make_re(Stat) ->
    repl(split_pattern(Stat, [])).

repl([single|T]) ->
    <<"[^_]*", (repl(T))/binary>>;
repl([double|T]) ->
    <<".*", (repl(T))/binary>>;
repl([H|T]) ->
    <<H/binary, (repl(T))/binary>>;
repl([]) ->
    <<>>.

split_pattern(<<>>, Acc) ->
    lists:reverse(Acc);
split_pattern(<<"**", T/binary>>, Acc) ->
    split_pattern(T, [double|Acc]);
split_pattern(<<"*", T/binary>>, Acc) ->
    split_pattern(T, [single|Acc]);
split_pattern(B, Acc) ->
    case binary:match(B, <<"*">>) of
        {Pos,_} ->
            <<Bef:Pos/binary, Rest/binary>> = B,
            split_pattern(Rest, [Bef|Acc]);
        nomatch ->
            lists:reverse([B|Acc])
    end.

legacy_search_cont(Re, Status, Type) ->
    Found = aliases_regexp_foldr([Re]),
    lists:foldl(
        fun({Entry, DPs}, Acc) ->
            case match_type(Entry, Type) of
                true ->
                    DPnames = [D || {D,_}<- DPs],
                    case get_datapoint(Entry, DPnames) of
                        Values when is_list(Values) ->
                            [{Entry, zip_values(Values, DPs)} | Acc];
                        disabled when Status == '_';
                            Status == disabled ->
                            [{Entry, zip_disabled(DPs)} | Acc];
                        _ ->
                            [{Entry, [{D, undefined}
                                || D <- DPnames]} | Acc]
                    end;
                false ->
                    Acc
            end
        end, [], orddict:to_list(Found)).

aliases_regexp_foldr([N]) ->
    exometer_alias:regexp_foldr(N,alias_fun(),orddict:new()).

alias_fun() ->
    fun(Alias, Entry, DP, Acc) ->
        orddict:append(Entry, {DP, Alias}, Acc)
    end.

match_type(_Name, Type) when Type == '_' ->
    true;
match_type(Name, Type) ->
    Type == exometer:info(Name, type).

get_datapoint(Entry, DPs) ->
    case exometer:get_value(Entry, DPs) of
        {ok, V} -> V;
        _ -> unavailable
    end.

zip_values([{D, V} | T], DPs) ->
    {_, N} = lists:keyfind(D, 1, DPs),
    [{D, V, N} | zip_values(T, DPs)];
zip_values([], _) ->
    [].

zip_disabled(DPs) -> [{D, disabled, N} || {D, N} <- DPs].

%%%-----------------------------------------------------------------------------
%% @doc Use the alias of the stat to find it's datapoint @end
%%%-----------------------------------------------------------------------------
-spec(find_through_alias([alias()]) -> [] | {alias(), datapoints()}).
find_through_alias([Alias]) when is_atom(Alias)->
    case exometer_alias:resolve(Alias) of
        error -> [];
        {Entry,DP} -> {Entry, DP}
    end;
find_through_alias(_Aliases) -> [].

%%%-----------------------------------------------------------------------------
%% @doc Print all the stats for the app stored in exometer/metadata @end
%%%-----------------------------------------------------------------------------
-spec(stats() -> found_entries()).
stats() -> get_stats([['_']]).

%%%-----------------------------------------------------------------------------
%% @doc
%% The Path is the start or full name of the stat(s) you wish to find,
%% i.e. [stat|'_'] as a path will return stats with those to elements in their
%% path. and uses exometer:find_entries
%% @end
%%%-----------------------------------------------------------------------------
-spec(get_values(stat_path()) -> found_values()).
get_values(Arg) -> exometer:get_values(Arg).

get_value(Name) -> exometer:get_value(Name).


%%%=============================================================================
%%%-----------------------------------------------------------------------------
%% @doc
%% For updating the stats, each of the stats module will call into this module
%% to update the stat in exometer by IncrVal given, all update function calls
%% call into the update_or_create/3 function in exometer. Meaning if the stat
%% has been deleted but is still hitting a function that updates it, it will
%% create a basic metric for that stat so the update values are stored somewhere.
%% This means if the stat is to be unregistered/deleted, its existence should be
%% removed from the stats code, otherwise it is to be disabled.
%% @end
%%%-----------------------------------------------------------------------------
-type incr_value() :: pos_integer() | float().
-spec(update(stat_name(), incr_value(), type()) -> ok).
update(Name, Val, Type) ->
    update(Name, Val, Type, []).
update(Name, Val, Type, Opts) ->
    exometer:update_or_create(Name, Val, Type, Opts).

%%%=============================================================================

%%%-----------------------------------------------------------------------------
%% @doc unregister a stat in metadata, and deletes the metric from exometer @end
%%%-----------------------------------------------------------------------------
-spec(unregister(stat_name()) -> ok).
unregister([Stats|Name]) when is_atom(Stats) ->
    unregister_stat([Stats|Name]);
unregister(Stats) -> %% generic
    lists:foreach(fun(Stat) -> unregister_stat(Stat) end, Stats).

unregister_stat(StatName) ->
    case riak_stats_metadata:enabled() of
        false ->
            exometer:delete(StatName);
        true ->
            riak_stats_metadata:unregister(StatName),
            exometer:delete(StatName)
    end.

%%%=============================================================================
%%%-----------------------------------------------------------------------------
%% @doc change status in metadata (if enabled) and then in exometer. @end
%%%-----------------------------------------------------------------------------
-type stats_status_list()   :: [{stat_name(), status()}] | [].
-spec(change_status(stats_status_list()) -> ok).
change_status([]) -> io:format("No stats need changing~n");
change_status(StatsList) ->
    case riak_stats_metadata:enabled() of
        false ->
            change_mem_status(StatsList);
        true ->
            riak_stats_metadata:change_status(StatsList),
            change_mem_status(StatsList)
    end.

change_mem_status(Stats) ->
    lists:foldl(fun({Stat,Status},Acc) ->
        exometer:setopts(Stat,[{status,Status}]),
        [{Stat,Status}|Acc]
                end, [], Stats).

-spec(reset(stat_name()) -> ok).
reset(Stat) -> exometer:reset(Stat).

aliases_prefix_foldl() ->
    exometer_alias:prefix_foldl(<<>>,alias_fun(),orddict:new()).

-spec(alias(Group :: orddict:orddict()) -> ok | any()).
alias(Group) ->
    lists:keysort(
        1,
        lists:foldl(
            fun({K, DPs}, Acc) ->
                case get_datapoint(K, [D || {D, _} <- DPs]) of
                    {ok, Vs} when is_list(Vs) ->
                        lists:foldr(fun({D, V}, Acc1) ->
                            {_, N} = lists:keyfind(D, 1, DPs),
                            [{N, V} | Acc1]
                                    end, Acc, Vs);
                    {ok,disabled} ->
                        [{N,undefined} || {_,N} <-DPs]++Acc;
                    _ ->
                        [{N,0} || {_,N} <-DPs]++Acc

                end
            end, [], orddict:to_list(Group))).

%%%=============================================================================
%%% Internal API
%%%=============================================================================

%%%-----------------------------------------------------------------------------
%% @doc Returns the timestamp to put in the stat entry  @end
%%%-----------------------------------------------------------------------------
-spec(timestamp() -> exometer_util:timestamp()).
timestamp() ->
    exometer_util:timestamp().

