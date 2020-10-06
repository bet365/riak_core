%%%-----------------------------------------------------------------------------
%%% @doc Handle console input for stats, profiles and push configuration @end
%%%-----------------------------------------------------------------------------
-module(riak_stats_console).
-include("riak_stats.hrl").
-include("riak_stats_push.hrl").

-export([
    %% Stat API
    show/1,                     metadata/1,
    info/1,                     update_status/2,
    enable/1,                   sanitise_stat_input/1,
    disable/1,                  sanitise_stat_input/2,
    reset/1,                    sanitise_stat_input/3,

    %% Profile API
    save_profile/1,             load_profile/1,
    load_profile_all/1,         delete_profile/1,
    reset_profile/0,            reset_profile_all/0,
    get_profile/1,              get_all_profiles/0,
    get_all_loaded_profiles/0,  get_loaded_profile/0,

    %% Push API
    start/1,                    find_push_stats/1,
    stop/1,                     find_push_stats_all/1,
    sanitise_push_input/1,

    %% Other API
    print/1, print/2]).

%% all the attributes to return in 'stat info <stat>'

%%%=============================================================================
%%% API
%%%=============================================================================
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% Stats Functions %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%-----------------------------------------------------------------------------
%% @doc Show stat(s) and their value(s) @end
%%%-----------------------------------------------------------------------------
-spec(show(console_arg()) -> no_return()).
show(Arg) ->
    SanitisedStats = sanitise_stat_input(Arg),
    Found = find_entries(SanitisedStats),
    print(Found).

%%%-----------------------------------------------------------------------------
%% @doc Returns all the stats information @end
%%%-----------------------------------------------------------------------------
-spec(info(console_arg()) -> no_return()).
info(Arg) ->
    {Attributes, RestArg} = pick_info_attrs(Arg),
    {Stat, Type, Status, _DataPoints} = sanitise_stat_input(RestArg),
    %% replace the Datapoints with the attributes given
    SanitisedStats = {Stat, Type, Status, Attributes},
    Found = find_entries(SanitisedStats),
    print(Found).

%%%-----------------------------------------------------------------------------
%% @doc get list of attrs to return @end
%%%-----------------------------------------------------------------------------
-spec(pick_info_attrs(console_arg()) -> {attributes(), console_arg()}).
pick_info_attrs(Arg) ->
    Fun = get_attr_fun(),
    Acc = {[], []},
    %% Attributes are defined by "-" in front of them (in cmd line)
    AttrAndStats = re:split(Arg, "\\-", [{return, list}]),
    case lists:foldr(Fun, Acc, AttrAndStats) of
        {[], Rest} ->           %% If no arguments given
            {?INFO, Rest};      %% use all, and return arg
        Other ->
            Other
    end.

get_attr_fun() ->
    fun
        ("name",     {Attrs, StatArg}) -> {[name      | Attrs], StatArg};
        ("type",     {Attrs, StatArg}) -> {[type      | Attrs], StatArg};
        ("module",   {Attrs, StatArg}) -> {[module    | Attrs], StatArg};
        ("value",    {Attrs, StatArg}) -> {[value     | Attrs], StatArg};
        ("cache",    {Attrs, StatArg}) -> {[cache     | Attrs], StatArg};
        ("status",   {Attrs, StatArg}) -> {[status    | Attrs], StatArg};
        ("timestamp",{Attrs, StatArg}) -> {[timestamp | Attrs], StatArg};
        ("options",  {Attrs, StatArg}) -> {[options   | Attrs], StatArg};
        (Other,      {Attrs, StatArg}) -> {Attrs,     [Other | StatArg]}
    end.

%%%-----------------------------------------------------------------------------
%% @doc enable the stats, if the stat is already enabled does nothing @end
%%%-----------------------------------------------------------------------------
-spec(enable(console_arg()) -> no_return()).
enable(Arg) ->
    EnabledStats = update_status(Arg, enabled),
    print(EnabledStats).

%%%-----------------------------------------------------------------------------
%% @doc disable the stats - if already disabled does nothing @end
%%%-----------------------------------------------------------------------------
-spec(disable(console_arg()) -> no_return()).
disable(Arg) ->
    DisabledStats = update_status(Arg, disabled),
    print(DisabledStats).

%%%-----------------------------------------------------------------------------
%% @doc
%% change the status of the stat (in metadata and) in exometer. Pulls only the
%% enabled stats if the status(es) are to be disabled, and vice versa.
%% @end
%%%-----------------------------------------------------------------------------
-spec(update_status(console_arg(), status()) -> no_return()).
update_status(Arg, ToStatus) ->
    {Entries, _DataPoints} =
        case ToStatus of
            enabled  -> find_entries(sanitise_stat_input(Arg, '_', disabled));
            disabled -> find_entries(sanitise_stat_input(Arg, '_', enabled))
        end,
    riak_stats_metadata:change_status([{Stat, ToStatus} || {Stat, _, _} <- Entries]).

%%%-----------------------------------------------------------------------------
%% @doc resets the stats in metadata and exometer @end
%%%-----------------------------------------------------------------------------
-spec(reset(console_arg()) -> ok).
reset(Arg) ->
    SanitisedStats = sanitise_stat_input(Arg),
    {Found, _DataPoints} = find_entries(SanitisedStats),
    lists:foreach(fun({Name, _, _}) -> exometer:reset(Name) end, Found).

%%%-----------------------------------------------------------------------------
%% @doc enabling the metadata allows the stats configuration to be persisted,
%% disabling the metadata means only using the exometer functions. @end
%%%-----------------------------------------------------------------------------
%% @doc
%% Upon re-enabling the metadata will "re-register" the stats, to remain
%% consistent between both metadata and exometer, any profile that was loaded
%% before will not be loaded upon re-enabling to prevent errors
%% @end
%%%-----------------------------------------------------------------------------
-type metadata_arg() :: list() | enabled | disabled | status.
-spec(metadata(metadata_arg()) -> ok).
metadata(["enable" ]) -> metadata(enabled);
metadata(["disable"]) -> metadata(disabled);
metadata(["status" ]) -> metadata_status();
metadata(Arg) when is_atom(Arg) ->
    case Arg == riak_stats_metadata:enabled() of
        true  -> metadata_status();
        false -> set_metadata(Arg)
    end.

metadata_status() ->
    print_response("Metadata is ~p~n",[riak_stats_metadata:enabled()]).

set_metadata(enabled) ->
    riak_stats_metadata:reload_metadata(),
    metadata_env(enabled);
set_metadata(disabled) ->
    metadata_env(disabled).

metadata_env(Status) ->
    application:set_env(?PERSIST_APP, ?PERSIST_ENV, Status),
    metadata_status().

%%%=============================================================================
%%% Helper API
%%%=============================================================================
-define(STATUS, enabled). %% default status
-define(TYPE,   '_').     %% default type (anything)
-define(DPs,    default). %% default Datapoints
%%%-----------------------------------------------------------------------------
%%% @doc
%%% Arguments coming in from the console arrive at this function for stats, data
%%% is transformed into a metrics name and type, status/datapoints if they have
%%% been given.
%%% @end
%%%-----------------------------------------------------------------------------
-spec(sanitise_stat_input(console_arg()) -> sanitised_stat()).
sanitise_stat_input(Arg) ->
    parse_stat_entry(check_args(Arg), ?TYPE, ?STATUS, ?DPs).

%% separate Status from Type with function of same arity
sanitise_stat_input(Arg, Status)
    when   (Status == enabled)
    orelse (Status == disabled)
    orelse (Status == '_') ->
    parse_stat_entry(check_args(Arg), ?TYPE, Status, ?DPs);
%% If doesn't match the clause above, it must be the Type given.
sanitise_stat_input(Arg, Type) ->
    parse_stat_entry(check_args(Arg), Type, ?STATUS, ?DPs).

sanitise_stat_input(Arg, Type, Status) ->
    parse_stat_entry(check_args(Arg), Type,  Status, ?DPs).

%%%-----------------------------------------------------------------------------
%% @doc make sure all Args are Binaries in a list: [<<Args>>] sanitised for
%% parse_stat_entry @end
%%%-----------------------------------------------------------------------------
check_args([Args]) when is_atom(Args) ->
    check_args(atom_to_binary(Args, latin1));
check_args([Args]) when is_list(Args) ->
    check_args(list_to_binary(Args));
check_args([Args]) when is_binary(Args) ->
    [Args];

check_args(Args) when is_atom(Args) ->
    check_args(atom_to_binary(Args, latin1));
check_args(Args) when is_list(Args) ->
    check_args(
        lists:map(fun
                      (A) when is_atom(A) ->
                          atom_to_binary(A, latin1);
                      (A) when is_list(A) ->
                          list_to_binary(A);
                      (A) when is_binary(A) ->
                          A
                  end, Args));
check_args(Args) when is_binary(Args) ->
    [Args];
check_args(_) ->
    print("Illegal Argument Type ~n"), [].

parse_stat_entry(BinArgs, Type, Status, DPs) ->
    [Bin | Args] = re:split(BinArgs, "/"), %% separate /type=*.. etc...
    {NewType, NewStatus, NewDPs} =
        %% pull the type, status and datapoints from the argument given
    %% if nothing was given then the default is returned.
    type_status_and_dps(Args, Type, Status, DPs),
    StatName = statname(Bin),
    {StatName, NewStatus, NewType, NewDPs}.

type_status_and_dps([], Type, Status, DPs) ->
    {Type, Status, DPs};
type_status_and_dps([<<"type=", T/binary>> | Rest], _T, Status, DPs) ->
    NewType =
        case T of
            <<"*">> -> '_';
            _ ->
                try binary_to_existing_atom(T, latin1)
                catch error:_ -> T
                end
        end,
    type_status_and_dps(Rest, NewType, Status, DPs);
type_status_and_dps([<<"status=", S/binary>> | Rest], Type, _Status, DPs) ->
    NewStatus =
        case S of
            <<"*">> -> '_';
            <<"enabled">>  -> enabled;
            <<"disabled">> -> disabled;
            _ -> enabled
        end,
    type_status_and_dps(Rest, Type, NewStatus, DPs);
type_status_and_dps([DPsBin | Rest], Type, Status, DPs) ->
    Atoms =
        lists:foldl(fun(D,Acc) ->
            try list_to_integer(D) of
                DP -> [DP|Acc]
            catch _:_ ->
                try list_to_atom(D) of
                    DP2 ->
                        [DP2|Acc]
                catch _:_ ->
                    io:fwrite("Illegal datapoint name~n"),
                    Acc
                end
            end
                    end,[], re:split(DPsBin,",",[{return,list}])),
    NewDPs = merge(lists:flatten(Atoms),DPs),
    type_status_and_dps(Rest, Type, Status, NewDPs).

%% @doc If 'default' then
%% it will be given as 'default' to exometer anyway
merge([_ | _] = DPs, default) ->
    DPs;
%% Otherwise checks if the H from Arg is a member of the DPs list,
%% and will skip if they are already in there. @end
merge([H | T], DPs) ->
    case lists:member(H, DPs) of
        true -> merge(T, DPs);
        false -> merge(T, DPs ++ [H])
    end;
merge([], DPs) ->
    DPs.

%% @doc creates a path for the stat name @end
statname([]) ->
    ['_'] ;
statname("*") ->
    statname([]);
statname("["++_ = Expr) ->
    case erl_scan:string(ensure_trailing_dot(Expr)) of
        {ok, Toks, _} ->
            case erl_parse:parse_exprs(Toks) of
                {ok, [Abst]} -> partial_eval(Abst);
                Error -> print("Parse error in ~p for ~p~n",
                    [Expr, Error]), []
            end;
        Error -> print("Scan Error in ~p for ~p~n",
            [Expr, Error]), []
    end;
statname(Arg) when is_binary(Arg) ->
    Parts = re:split(Arg, "\\.", [{return,list}]),
    replace_parts(Parts);
statname(_) ->
    lager:error("Illegal Argument Type~n").

ensure_trailing_dot(Str) ->
    case lists:reverse(Str) of
        "." ++ _ ->
            Str;
        _ ->
            Str ++ "."
    end.

partial_eval({cons, _, H, T}) ->
    [partial_eval(H) | partial_eval(T)];
partial_eval({tuple, _, Elems}) ->
    list_to_tuple([partial_eval(E) || E <- Elems]);
partial_eval({op, _, '++', L1, L2}) ->
    partial_eval(L1) ++ partial_eval(L2);
partial_eval(X) ->
    erl_parse:normalise(X).

replace_parts(Parts) ->
    case split(Parts, "**", []) of
        {_, []} ->
            [replace_parts_1(Parts)];
        {Before, After} ->
            Head = replace_parts_1(Before),
            Tail = replace_parts_1(After),
            [Head ++ Pad ++ Tail || Pad <- pads()]
        %% case of "**" in between elements in metric name
        %% a Pad ('_') is added in between the terms given
        %% in the arg, up to 10 times, and inserted into the
        %% list; as in:
        %% [[stat,'_',end],
        %% [stat,'_','_',end],
        %% [stat,'_','_','_',end],...];
        %% If the Argument: stat.**.end was given.
    end.

split([H | T], H, Acc) ->
    {lists:reverse(Acc), T};
split([H | T], X, Acc) ->
    split(T, X, [H | Acc]);
split([], _, Acc) ->
    {lists:reverse(Acc), []}.

replace_parts_1([H | T]) ->
    R = replace_part(H),
    case T of
        O when O == "**"; O == '_' -> '_';
        ["**"] -> [R] ++ '_'; %% [stat|'_']
        _ -> [R | replace_parts_1(T)]
    end;
replace_parts_1([]) ->
    [].

replace_part(H) ->
    case H of
        O when O == "*"; O == '_' -> '_';
        "'" ++ _ ->
            case erl_scan:string(H) of
                {ok, [{atom, _, A}], _} ->
                    A;
                Error ->
                    lager:error("Cannot replace part: ~p~n",
                        [Error])
            end;
        [C | _] when C >= $0, C =< $9 ->
            try list_to_integer(H)
            catch
                error:_ -> list_to_atom(H)
            end;
        _ -> list_to_atom(H)
    end.

pads() -> [lists:duplicate(N, '_') || N <- lists:seq(1,10)].

%%%-----------------------------------------------------------------------------
%% @doc Find_entries for the stat show/info. @end
%%%-----------------------------------------------------------------------------
-spec(find_entries(sanitised_stat()|console_arg()) -> found_entries()).
find_entries({Stat, Status, Type, DPs}) ->
    find_entries(Stat, Status, Type, DPs).
find_entries(Stats, Status, Type, default) ->
    find_entries(Stats, Status, Type, []);
find_entries(Stats, Status, Type, DPs) ->
    riak_stats:find_entries(Stats, Status, Type, DPs).

%%%-----------------------------------------------------------------------------

get_value(Name) ->
    Values = riak_stats:get_value(Name),
    Folded = fold_values(Values),
    print_stat_args(Name, Folded).

find_stats_info({Name, Type, Status}, Info) ->
    Values = riak_stats:get_datapoint(Name, Info),
    Folded = fold_values(Values),
    print_stat_args({Name, Type, Status, Info},Folded).

get_info_2(Statname, Attrs) ->
    fold_values([exometer:info(Statname, Attrs)]).

print_stat_args(_StatName, []) -> [];
print_stat_args(StatName, disabled) ->
    io:fwrite("~p : disabled~n", [StatName]);
print_stat_args({StatName, Type, Status, Info},{error,_}) ->
    NewValues = get_info_2(StatName, Info),
    NewArgs = replace_type_and_status(Type, Status, NewValues),
    print_stat_args(StatName, NewArgs);
print_stat_args({Statname, Type, Status,_Info}, Args) ->
    NewArgs = replace_type_and_status(Type, Status, Args),
    print_stat_args(Statname, NewArgs);
print_stat_args(StatName, Args) ->
    io:fwrite("~p : ~p~n",[StatName, Args]).

fold_values([]) -> [];
fold_values(Values) when is_list(Values) ->
    lists:foldl(fun
                    (undefined, A)            -> A;
                    ([], A)                   -> A;
                    ({type, _}, A)            -> [{type, undef} | A];
                    ({status, _}, A)          -> [{status, undef} | A];
                    ({_, undefined}, A)       -> A;
                    ({_, {error, _}}, A)      -> A;
                    ({ms_since_reset, _V}, A) -> A;
                    (DP, A)                   -> [DP | A]
                end, [], Values);
fold_values({ok, Values}) -> fold_values(Values);
fold_values(Values) -> Values.

replace_type_and_status(Type, Status, List) ->
    NewList = lists:keyreplace(type, 1, List, {type, Type}),
    lists:keyreplace(status, 1, NewList, {status, Status}).

print_response(String, Args) -> io:fwrite(String, Args).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%% Profile Functions %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @see riak_stats_profiles.erl @doc
-spec(save_profile(profile_name()) -> ok).
save_profile(ProfileName) ->
    ConcatStrName = sanitise_profile_input(ProfileName),
    print_profile("Saved",
        case profile_enabled() of
            true ->
                riak_stats_profiles:save_profile(ConcatStrName);
            False ->
                {False, ConcatStrName}
        end).

-spec(load_profile(profile_name()) -> ok).
load_profile(ProfileName) ->
    load_profile(ProfileName, [node()]).
load_profile(ProfileName, Node) ->
    ConcatStrName = sanitise_profile_input(ProfileName),
    print_profile("Loaded",
        case profile_enabled() of
            true ->
                riak_stats_profiles:load_profile(ConcatStrName, Node);
            False ->
                {False, ConcatStrName}
        end).

-spec(load_profile_all(profile_name()) -> ok).
load_profile_all(ProfileName) ->
    ConcatStrName = sanitise_profile_input(ProfileName),
    Nodes = [node()|nodes()],
    load_profile(ConcatStrName, Nodes).

-spec(get_profile(profile_name()) -> ok).
get_profile(ProfileName) ->
    ConcatStrName = sanitise_profile_input(ProfileName),
    print_profile("Found",
        case profile_enabled() of
            true ->
                case riak_stats_profiles:get_profile(ConcatStrName) of
                    {error, Reason} ->
                        {{error, Reason}, ConcatStrName};
                    _ ->
                        {ok, ConcatStrName}
                end;
            False ->
                {False, ConcatStrName}
        end).

-spec(get_all_profiles() -> ok).
get_all_profiles() ->
    print_profile("Found",
        case profile_enabled() of
            true ->
                {ok, [Profile ||
                    {Profile, _Stats} <- riak_stats_profiles:get_all_profiles()]};
            False ->
                {False, " "}
        end).

-spec(get_all_loaded_profiles() -> ok).
get_all_loaded_profiles() ->
    print_profile("Found",
        case profile_enabled() of
            true ->
                Nodes = [node()|nodes()],
                {ok, [get_loaded_profile(Node) || Node <- Nodes]};
            False ->
                {False, " "}
        end).

get_loaded_profile() ->
    get_loaded_profile(node()).
get_loaded_profile(Node) ->
    print_profile("Found",
        case profile_enabled() of
            true ->
                {ok, riak_stats_profiles:get_loaded_profile(Node)};
            False ->
                {False, " "}
        end).

-spec(delete_profile(profile_name()) -> ok).
delete_profile(ProfileName) ->
    ConcatStrName = sanitise_profile_input(ProfileName),
    print_profile("Deleted",
        case profile_enabled() of
            true -> riak_stats_profiles:delete_profile(ConcatStrName);
            False ->
                {False, ConcatStrName}
        end).

-spec(reset_profile() -> no_return()).
-spec(reset_profile([node()]) -> ok).
reset_profile() ->
    reset_profile([node()]).
reset_profile(Nodes) ->
    print_profile("Reset",
        case profile_enabled() of
            true ->
                riak_stats_profiles:reset_profile(Nodes);
            False ->
                {False," "}
        end).

reset_profile_all() ->
    reset_profile([node()|nodes()]).

profile_enabled() ->
    case riak_stats_metadata:enabled() of
        false -> {error, persistence_disabled};
        true -> true
    end.

sanitise_profile_input(ProfileName) ->
    %% if multiple arguments are passed in from the console the name is a list
    %% of string elements -> make this all one string, not ignoring the intended
    %% spaces.
    string:join(ProfileName, " ").

print_profile(Action, {ok, ProfileName}) ->
    io:fwrite("Profile ~s : ~p ~n", [Action, ProfileName]);
print_profile(Action, {{error, Reason}, ProfileName}) ->
    io:fwrite("Error, not ~s ~p -> because : ~p",[Action, ProfileName, Reason]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% Push Functions %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%-----------------------------------------------------------------------------
%% @doc Setup a gen_server to push stats to an endpoint
%% @see riak_stats_push:maybe_start_server/2 @end
%%%-----------------------------------------------------------------------------
-spec(start(console_arg()) -> ok).
start(ListofArgs) ->
    {Protocol, Data} = sanitise_push_input(ListofArgs),
    riak_stats_push:maybe_start_server(Protocol, Data).

%%%-----------------------------------------------------------------------------
%% @doc kill the gen_server that is pushing stats to an endpoint,
%% @see riak_stats_push:terminate_server/2 @end
%%%-----------------------------------------------------------------------------
-spec(stop(console_arg()) -> ok).
stop(ListofArgs) ->
    {Protocol, Data} = sanitise_push_input(ListofArgs),
    riak_stats_push:terminate_server(Protocol, Data).

%%------------------------------------------------------------------------------
%% @doc
%% Sanitise the data coming in, into the necessary arguments for setting
%% up an endpoint, This is specific to the endpoint functions
%% @end
%%------------------------------------------------------------------------------
-spec(sanitise_push_input(console_arg()) -> ok | {protocol(),sanitised_push()}).
sanitise_push_input(ListofArgs) ->
    case list_to_tuple(ListofArgs) of
        {HostPort, Protocol, Instance} ->
            sanitise_push(HostPort, Protocol, Instance, ['_']);
        {HostPort, Protocol, Instance, Stats} ->
            sanitise_push(HostPort, Protocol, Instance, Stats);
        _ -> lager:error("Invalid number of Arguments"), ok
    end.

sanitise_push(HostPort, Protocol, Instance, Stats) ->
    {Host0, Port0} = get_host_port(HostPort),
    TheHost = get_host(Host0),
    ThePort = get_port(Port0),
    TheProtocol = list_to_atom(Protocol),
    TheInstance = Instance,
    {TheStats, _, _, _} = sanitise_stat_input(Stats),
    {TheProtocol, {{ThePort, TheInstance, TheHost}, TheStats}}.

get_host_port(HostPort) ->
    [Host|Port] = re:split(HostPort,"\\:",[{return,list}]),
    {Host,Port}.

get_host(Host) ->
    case string:find(Host, ".") of
        nomatch -> %% not ip address
            Hostname = Host,
            Hostname;
        IP -> % It is an ip address
            IpAddr = inet:parse_ipv4_address(IP),
            IpAddr %% made into ipv4 address (tuple)
    end.

get_port([Port]) -> get_port(Port);
get_port(Port) -> list_to_integer(Port).


%%%-----------------------------------------------------------------------------
%% @doc find information about stats that are being pushed on all nodes @end
%%%-----------------------------------------------------------------------------
-spec(find_push_stats_all(console_arg()) -> ok).
find_push_stats_all(Arg) ->
    Sanitised = sanitise_push_input(Arg),
    print_info(riak_stats_push:find_push_stats([node() | nodes()], Sanitised)).

%%%-----------------------------------------------------------------------------
%% @doc find information about stats that are being pushed on this node @end
%%%-----------------------------------------------------------------------------
-spec(find_push_stats(console_arg()) -> ok).
find_push_stats(Arg) ->
    Sanitised = sanitise_push_input(Arg),
    print_info(riak_stats_push:find_push_stats([node()], Sanitised)).


-spec(print_info(push_arg() | any()) -> ok).
print_info([]) ->
    io:fwrite("Nothing found~n");
print_info(Info) ->
    String = "~10s ~8s ~-8s ~-15s ~-10s  ~-8s  ~-16s ~-16s ~-6s ~-8s ~s~n",
    ColumnArgs =
        ["LastDate","LastTime","Protocol","Name","Date","Time",
            "Node","ServerIp","Port","Running?","Stats"],
    io:format(String, ColumnArgs),
    lists:foreach(
        fun
            ({{Protocol, Instance},
                #{original_dt   := OriginalDateTime,
                    modified_dt := ModifiedDateTime,
                    running     := Running,
                    node        := Node,
                    port        := Port,
                    server_ip   := Sip,
                    stats       := Stats}}) ->
                {ODate,OTime} = OriginalDateTime,
                {MDate,MTime} = ModifiedDateTime,
                LastDate = consistent_date(MDate),
                LastTime = consistent_time(MTime),
                SProtocol = atom_to_list(Protocol),
                SInstance = Instance,
                Date = consistent_date(ODate),
                Time = consistent_time(OTime),
                SNode = atom_to_list(Node),
                ServerIp = Sip,
                SPort = integer_to_list(Port),
                SRunning = atom_to_list(Running),
                StatsString = io_lib:format("~p",[Stats]),
                Args = [LastDate, LastTime, SProtocol, SInstance, Date,
                    Time, SNode, ServerIp, SPort, SRunning, StatsString],
                io:format(String, Args);
            (_Other) -> ok
        end, Info).

%% the dates and time when printed have a different length, due to the
%% integer being < 10, so the lack of digits makes the list of information
%% printed out have a wonky effect.
%% This is just another layer of pretty printing.

consistent_date(Date) when is_tuple(Date) ->
    consistent_date(tuple_to_list(Date));
consistent_date(Date) when is_list(Date) ->
    NewDate = integers_to_strings(Date),
    io_lib:format("~s/~s/~s ",NewDate).

consistent_time(Time) when is_tuple(Time) ->
    consistent_time(tuple_to_list(Time));
consistent_time(Time) when is_list(Time)->
    NewTime = integers_to_strings(Time),
    io_lib:format("~s:~s:~s ",NewTime).

integers_to_strings(IntegerList) ->
    lists:map(fun(Num) when length(Num) < 2 -> "0"++Num;
        (Num) -> Num
              end, [integer_to_list(N) || N <- IntegerList]).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% Other Functions %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec(print(atom()|string()|list()|found_stats()|any()) -> term()).
print(undefined)    -> print([]);
print([undefined])  -> print([]);
print({Stats, DPs}) -> print(Stats, DPs);
print(Arg)          -> print(Arg, []).

print([], _) ->
    io:fwrite("No Matching Stats~n");
print(NewStats, Args) ->
    lists:map(fun

                  ({N,_T,_S}) when Args == [] -> get_value(N);
                  ({N, T, S}) ->  find_stats_info({N, T, S}, Args);

                  %% legacy pattern
                  (Legacy) -> legacy_map(Legacy)
              end, NewStats).

legacy_map(Legacy) ->
    lists:map(fun
                  ({LP,[]}) ->
                      io:fwrite(
                          "== ~s (Legacy pattern): No matching stats ==~n",
                          [LP]);
                  ({LP, Matches}) ->
                      io:fwrite("== ~s (Legacy pattern): ==~n",
                          [LP]),
                      [[io:fwrite("~p: ~p (~p/~p)~n",
                          [N, V, E, DP]) ||
                          {DP, V, N} <- LDPs] ||
                          {E, LDPs} <- Matches];
                  (_) ->
                      []
              end, Legacy).
