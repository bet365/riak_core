%%%-----------------------------------------------------------------------------
%%% @doc
%%% Functions called by the TCP and UDP gen_servers and as well as the wm module
%%% @end
%%%-----------------------------------------------------------------------------
-module(riak_stats_push_util).

-export([json_stats/1]).

%%%-----------------------------------------------------------------------------
%% @doc
%% called by push_stats/4-5 in stats_push_tcp/udp to collect stats from
%% get_stats/1 and convert them to json objects
%% @end
%%%-----------------------------------------------------------------------------
json_stats(Stats) ->
    JsonStats = encode(get_stats(Stats)),
    DateTime = format_time(os:timestamp()),
    [${, quote("timestamp"), $:, quote(DateTime), $,, JsonStats, $}, "\n"].

encode(Metrics) ->
    New = lists:foldl(fun({Name, Value}, Acc) ->
        case proplists:get_value(value, Value) of
            {error, not_found} -> Acc;
            _Other -> [{Name, Value} | Acc]
        end
                      end, [], Metrics),
    mochijson2:encode({struct, New}).

%% @doc surround argument with quotes @end
quote(X) ->
    [$", X, $"].

-define(MILLISECONDS_MICROSECONDS_DIVISOR,  1000).
-define(SECS_MILLISECOND_DIVISOR,           1000).

%%------------------------------------------------------------------------------
%% @doc
%% For a given timestamp, returns the string representation in the following
%% format, YYYY.MM.ddTHH:mm:ss.SSSZ the time is in UTC.
%% @end
%%------------------------------------------------------------------------------
-spec(format_time(erlang:timestamp()) -> string()).
format_time(Now) ->
    DateTime = calendar:now_to_universal_time(Now),
    httpd_util:rfc1123_date(DateTime).

%%%-----------------------------------------------------------------------------
%% @doc
%% Get the stats from exometer and convert them into json objects for both
%% stats_push_tcp/udp and stats_wm.
%% @end
%%%-----------------------------------------------------------------------------
get_stats(Arg) ->
    Stats = riak_stats:get_values(Arg),
    StringList =
        lists:map(fun({N,V}) ->
            NewName = parse_name(N),
            NewValues = parse_values(V),
            {NewName,NewValues}
                  end, Stats),
    StringList.

parse_name(Name) ->
    [App | Stat] = Name,
    S = atom_to_list(App),
    T = ": ",
    R = stringifier(Stat),
    S++T++R.

stringifier(Stat) ->
    lists:foldr(fun
                    ({IpAddr, Port}, Acc) when is_tuple(IpAddr) ->
                        T = inet:ntoa(IpAddr),
                        P = integer_to_list(Port),
                        "{" ++ T ++ "," ++ P ++ "}" ++ " " ++ Acc;
                    ({IpAddr, Port}, Acc) when is_list(IpAddr) ->
                        T = IpAddr,
                        P = integer_to_list(Port),
                        "{" ++ T ++ "," ++ P ++ "}" ++ " " ++ Acc;
                    (S,Acc) when is_list(S)    -> S++" "++Acc;
                    (I,Acc) when is_integer(I) -> integer_to_list(I)++" "++Acc;
                    (T,Acc) when is_tuple(T)   -> tuple_to_list(T)  ++" "++Acc;
                    (L,Acc) when is_atom(L)    -> atom_to_list(L)   ++" "++Acc;
                    (_Other,Acc) -> Acc
                end, " = ", Stat).

parse_values(Values) ->
    lists:foldl(fun
                    ({value,{ok,Vals}},Acc) -> [parse_values(Vals)|Acc];
                    ({ok,Vals},Acc)         -> [parse_values(Vals)|Acc];
                    ({ms_since_reset,_V},Acc) -> Acc;
                    (V,Acc) -> [V|Acc]
                end, [],Values).

