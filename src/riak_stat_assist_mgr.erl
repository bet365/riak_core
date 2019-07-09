%%%-------------------------------------------------------------------
%%% @copyright (C) 2019,
%%% @doc
%%%
%%% June 2019 update ->
%%%   consolidated Long winded functions
%%%   Functions used by many mngr modules
%%%   Commonly used functions are consolidated in this module.
%%%
%%% @end
%%% Created : 05. Jun 2019 13:31
%%%-------------------------------------------------------------------
-module(riak_stat_assist_mgr).

%% Migrated to riak_stat

%%-export([print_stats/2, print_stats0/1,just_print/1, just_print/2]).
%%%% API
%%-export([find_entries/2]).
%%
%%%%% ---- Stat show ---- %%%
%%-define(PFX, riak_stat_mngr:prefix()).

%%
%%-spec(print_stats(Entries :: term(), Attributes :: list() | term()) -> term() | ok).
%%%% @doc
%%%% Print stats is generic, and used by both stat show and stat info,
%%%% Stat info includes all the attributes that will be printed whereas stat show
%%%% will pass in an empty list into the Attributes field.
%%%% @end
%%print_stats([], _) ->
%%  io:fwrite("No matching stats~n");
%%print_stats({[{LP, []}], _}, _) ->
%%  io:fwrite("== ~s (Legacy pattern): No matching stats ==~n", [LP]);
%%print_stats({[{LP, Matches}], _}, []) ->
%%  io:fwrite("== ~s (Legacy pattern): ==~n", [LP]),
%%  [[io:fwrite("~p: ~p (~p/~p)~n", [N, V, E, DP])
%%    || {DP, V, N} <- DPs] || {E, DPs} <- Matches];
%%print_stats({[{LP, Matches}], _}, Attrs) ->
%%  io:fwrite("== ~s (Legacy pattern): ==~n", [LP]),
%%  lists:foreach(
%%    fun({N, _}) ->
%%      print_info_1(N, Attrs)
%%    end, Matches);
%%print_stats({[], _}, _) ->
%%  io_lib:fwrite("No matching stats~n", []);
%%print_stats({Entries, DPs}, []) ->
%%  [io:fwrite("~p: ~p~n", [E, get_value(E, Status, DPs)])
%%    || {E, _, Status} <- Entries];
%%print_stats({Entries, _}, Attrs) ->
%%  lists:foreach(
%%    fun({N, _, _}) ->
%%      print_info_1(N, Attrs)
%%    end, Entries);
%%%%print_stats([{Entries, DPS}], Att) ->
%%%%  print_stats({Entries, DPS}, Att);
%%print_stats(Data, Att) ->
%%  print_stats({[{Data, [], []}],[]}, Att).
%%
%%print_stats0(Stats) ->
%%  lists:foldl(
%%    fun(Stat, Acc) ->
%%      case prin_stat0(Stat) of
%%        {_H, disabled, _} ->
%%          Acc;
%%        {H, Status, _} ->
%%          [{H, Status} | Acc]
%%%%        {_H, _S, []} ->
%%%%          Acc;
%%%%        {_H, _S, _V} ->
%%%%          Acc
%%      end
%%%%      [prin_stat0(Stat) | Acc]
%%    end, [], Stats
%%  ).
%%
%%prin_stat0(Stat) ->
%%  H = lists:flatten(io_lib:fwrite("~p: ", [Stat])),
%%%%  Pad = lists:duplicate(length(H), $\s),
%%  Info = get_info(core, Stat),
%%  Status = io:fwrite("~w = ~p~n", [status, proplists:get_value(status, Info, enabled)]),
%%  Value = io:fwrite("~w = ~p~n", [value, proplists:get_value(value, Info)]),
%%%%  io:put_chars([H, Status, Value]).
%%  {H, Status, Value}.
%%
%%just_print(Stats) ->
%%  io:fwrite("Stats ~p~n~n",[length(Stats)]),
%%  lists:foreach(fun({Stat, _Val}) ->
%%    print_stats(find_entries(Stat, enabled), [value])
%%                end, Stats).
%%just_print(Stat, Status) ->
%%  io:fwrite("~p: ~p~n", [Stat, Status]).
%%
%%
%%% used to print the entire stat information
%%print_info_1(N, [A | Attrs]) ->
%%  Hdr = lists:flatten(io_lib:fwrite("~p: ", [N])),
%%  Pad = lists:duplicate(length(Hdr), $\s),
%%  Info = get_info(core, N),
%%  Status = proplists:get_value(status, Info, enabled),
%%  Body = [io_lib:fwrite("~w = ~p~n", [A, proplists:get_value(A, Info)])
%%    | lists:map(fun(value) ->
%%      io_lib:fwrite(Pad ++ "~w = ~p~n",
%%        [value, get_value(N, Status, default)]);
%%      (Ax) ->
%%        io_lib:fwrite(Pad ++ "~w = ~p~n",
%%          [Ax, proplists:get_value(Ax, Info)])
%%                end, Attrs)],
%%  io:put_chars([Hdr, Body]).
%%
%%get_value(_, disabled, _) ->
%%  disabled;
%%get_value(E, _Status, DPs) ->
%%  case get_datapoint(E, DPs) of
%%    {ok, V} -> V;
%%    {error, _} -> unavailable
%%  end.
%%
%%get_info(Name, Info) ->
%%  case riak_stat_mngr:get_info(Name, Info) of
%%    undefined ->
%%      [];
%%    Other ->
%%      Other
%%  end.
%%
%%aliases(Type, Entries) ->
%%  riak_stat_mngr:aliases(Type, Entries).
%%
%%get_datapoint(Name, DP) ->
%%  riak_stat_mngr:get_datapoint(Name, DP).
%%
%%%%                                       enabled  |  disabled
%%-spec(find_entries(Arg :: term()| list(), ToStatus :: atom()) ->
%%  ok | term() | {error, Reason :: term()}).
%%%% @doc
%%%% pulls the information of a stat out of exometer
%%%% @end
%%find_entries(Arg, ToStatus) ->
%%%%  lager:error("Arg: ~p ToStatus: ~p~n", [Arg, ToStatus]),
%%  lists:map(
%%    fun(A) ->
%%      {S, Type, Status, DPs} = type_status_and_dps(A, ToStatus),
%%      case S of
%%        "[" ++ _ ->
%%          {find_entries_1(S, Type, Status), DPs};
%%        _ ->
%%          case legacy_search(S, Type, Status) of
%%            false ->
%%              {find_entries_1(S, Type, Status), DPs};
%%            Found ->
%%              {Found, DPs}
%%          end
%%      end
%%    end, Arg).
%%
%%type_status_and_dps(S, ToStatus) ->
%%%%  lager:error("S: ~p ToStatus: ~p~n", [S, ToStatus]),
%%%%  [S1|Rest] = re:split(S, "/", [{return, list}]),
%%  [S1|Rest] = re:split(S, "/"),
%%%%  lager:error("S1: ~p Rest: ~p~n", [S1, Rest]),
%%  {Type, Status, DPs} = type_status_and_dps(Rest, '_', ToStatus, default),
%%%%  lager:error("Type: ~p Status: ~p DPs: ~p~n", [Type, Status, DPs]),
%%  {S1, Type, Status, DPs}.
%%
%%type_status_and_dps([<<"type=", T/binary>>|Rest], _Type, ToStatus, DPs) ->
%%  NewType = case T of
%%              <<"*">> -> '_';
%%              _ ->
%%                try binary_to_existing_atom(T, latin1)
%%                catch error:_ -> T
%%                end
%%            end,
%%  type_status_and_dps(Rest, NewType, ToStatus, DPs);
%%type_status_and_dps([<<"status=", St/binary>>|Rest], Type, _Status, DPs) ->
%%  NewStatus = case St of
%%                <<"enabled">>  -> enabled;
%%                <<"disabled">> -> disabled;
%%                <<"*">>        -> '_'
%%              end,
%%  type_status_and_dps(Rest, Type, NewStatus, DPs);
%%type_status_and_dps([DPsBin|Rest], Type, Status, DPs) ->
%%  NewDPs = merge([binary_to_existing_atom(D,latin1)
%%    || D <- re:split(DPsBin, ",")], DPs),
%%  type_status_and_dps(Rest, Type, Status, NewDPs);
%%type_status_and_dps([], Type, Status, DPs) ->
%%  {Type, Status, DPs}.
%%
%%merge([_|_] = DPs, default) ->
%%  DPs;
%%merge([H|T], DPs) ->
%%  case lists:member(H, DPs) of
%%    true  -> merge(T, DPs);
%%    false -> merge(T, DPs ++ [H])
%%  end;
%%merge([], DPs) ->
%%  DPs.
%%
%%find_entries_1(S, Type, Status) ->
%%  Patterns = lists:flatten([parse_stat_entry(S, Type, Status)]),
%%  riak_stat_mngr:select_stat(Patterns).
%%
%%parse_stat_entry([], Type, Status) ->
%%  {{[?PFX] ++ '_', Type, '_'}, [{'=:=','$status',Status}], ['$_']};
%%parse_stat_entry("*", Type, Status) ->
%%  parse_stat_entry([], Type, Status);
%%parse_stat_entry("[" ++ _ = Expr, _Type, _Status) ->
%%  case erl_scan:string(ensure_trailing_dot(Expr)) of
%%    {ok, Toks, _} ->
%%      case erl_parse:parse_exprs(Toks) of
%%        {ok, [Abst]} ->
%%          partial_eval(Abst);
%%        Error ->
%%          io:fwrite("(Parse error for ~p: ~p~n", [Expr, Error]),
%%          []
%%      end;
%%    ScanErr ->
%%      io:fwrite("(Scan error for ~p: ~p~n", [Expr, ScanErr]),
%%      []
%%  end;
%%parse_stat_entry(Str, Type, Status) when Status==enabled; Status==disabled ->
%%  Parts = re:split(Str, "\\.", [{return,list}]),
%%  Heads = replace_parts(Parts),
%%  [{{H,Type,Status}, [], ['$_']} || H <- Heads];
%%parse_stat_entry(Str, Type, '_') ->
%%  Parts = re:split(Str, "\\.", [{return,list}]),
%%  Heads = replace_parts(Parts),
%%  [{{H,Type,'_'}, [], ['$_']} || H <- Heads];
%%parse_stat_entry(_, _, Status) ->
%%  io:fwrite("(Illegal status: ~p~n", [Status]).
%%
%%ensure_trailing_dot(Str) ->
%%  case lists:reverse(Str) of
%%    "." ++ _ ->
%%      Str;
%%    _ ->
%%      Str ++ "."
%%  end.
%%
%%partial_eval({cons,_,H,T}) ->
%%  [partial_eval(H) | partial_eval(T)];
%%partial_eval({tuple,_,Elems}) ->
%%  list_to_tuple([partial_eval(E) || E <- Elems]);
%%partial_eval({op,_,'++',L1,L2}) ->
%%  partial_eval(L1) ++ partial_eval(L2);
%%partial_eval(X) ->
%%  erl_parse:normalise(X).
%%
%%replace_parts(Parts) ->
%%  case split("**", Parts) of
%%    {_, []} ->
%%      [replace_parts_1(Parts)];
%%    {Before, After} ->
%%      Head = replace_parts_1(Before),
%%      Tail = replace_parts_1(After),
%%      [Head ++ Pad ++ Tail || Pad <- pads()]
%%  end.
%%
%%split(X, L) ->
%%  split(L, X, []).
%%
%%split([H|T], H, Acc) ->
%%  {lists:reverse(Acc), T};
%%split([H|T], X, Acc) ->
%%  split(T, X, [H|Acc]);
%%split([], _, Acc) ->
%%  {lists:reverse(Acc), []}.
%%
%%replace_parts_1([H|T]) ->
%%  R = replace_part(H),
%%  case T of
%%    ["**"] -> [R] ++ '_';
%%    _ -> [R|replace_parts_1(T)]
%%  end;
%%replace_parts_1([]) ->
%%  [].
%%
%%replace_part(H) ->
%%  case H of
%%    "*" -> '_';
%%    "'" ++ _ ->
%%      case erl_scan:string(H) of
%%        {ok, [{atom, _, A}], _} ->
%%          A;
%%        Error ->
%%          error(Error)
%%      end;
%%    [C|_] when C >= $0, C =< $9 ->
%%      try list_to_integer(H)
%%      catch
%%        error:_ -> list_to_atom(H)
%%      end;
%%    _ -> list_to_atom(H)
%%  end.
%%
%%pads() ->
%%  [['_'],
%%    ['_','_'],
%%    ['_','_','_'],
%%    ['_','_','_','_'],
%%    ['_','_','_','_','_'],
%%    ['_','_','_','_','_','_'],
%%    ['_','_','_','_','_','_','_'],
%%    ['_','_','_','_','_','_','_','_'],
%%    ['_','_','_','_','_','_','_','_','_'],
%%    ['_','_','_','_','_','_','_','_','_','_'],
%%    ['_','_','_','_','_','_','_','_','_','_','_'],
%%    ['_','_','_','_','_','_','_','_','_','_','_','_'],
%%    ['_','_','_','_','_','_','_','_','_','_','_','_','_'],
%%    ['_','_','_','_','_','_','_','_','_','_','_','_','_','_'],
%%    ['_','_','_','_','_','_','_','_','_','_','_','_','_','_','_'],
%%    ['_','_','_','_','_','_','_','_','_','_','_','_','_','_','_','_']].
%%
%%legacy_search(S, Type, Status) ->
%%  case re:run(S, "\\.", []) of
%%    {match,_} ->
%%      false;
%%    nomatch ->
%%      Re = <<"^", (make_re(S))/binary, "$">>,
%%      [{S, legacy_search_1(Re, Type, Status)}]
%%  end.
%%
%%make_re(S) ->
%%  repl(split_pattern(S, [])).
%%
%%legacy_search_1(N, Type, Status) ->
%%  Found = aliases(regexp_foldr, [N]),
%%  lists:foldr(
%%    fun({Entry, DPs}, Acc) ->
%%      case match_type(Entry, Type) of
%%        true ->
%%          DPnames = [D || {D,_} <- DPs],
%%          case get_datapoint(Entry, DPnames) of
%%            {ok, Values} when is_list(Values) ->
%%              [{Entry, zip_values(Values, DPs)} | Acc];
%%            {ok, disabled} when Status=='_';
%%              Status==disabled ->
%%              [{Entry, zip_disabled(DPs)} | Acc];
%%            _ ->
%%              [{Entry, [{D,undefined} || D <- DPnames]}|Acc]
%%          end;
%%        false ->
%%          Acc
%%      end
%%    end, [], orddict:to_list(Found)).
%%
%%match_type(_, '_') ->
%%  true;
%%match_type(Name, T) ->
%%  T == get_info(Name, type).
%%
%%zip_values([{D,V}|T], DPs) ->
%%  {_,N} = lists:keyfind(D, 1, DPs),
%%  [{D,V,N}|zip_values(T, DPs)];
%%zip_values([], _) ->
%%  [].
%%
%%zip_disabled(DPs) ->
%%  [{D,disabled,N} || {D,N} <- DPs].
%%
%%repl([single|T]) ->
%%  <<"[^_]*", (repl(T))/binary>>;
%%repl([double|T]) ->
%%  <<".*", (repl(T))/binary>>;
%%repl([H|T]) ->
%%  <<H/binary, (repl(T))/binary>>;
%%repl([]) ->
%%  <<>>.
%%
%%split_pattern(<<>>, Acc) ->
%%  lists:reverse(Acc);
%%split_pattern(<<"**", T/binary>>, Acc) ->
%%  split_pattern(T, [double|Acc]);
%%split_pattern(<<"*", T/binary>>, Acc) ->
%%  split_pattern(T, [single|Acc]);
%%split_pattern(B, Acc) ->
%%  case binary:match(B, <<"*">>) of
%%    {Pos,_} ->
%%      <<Bef:Pos/binary, Rest/binary>> = B,
%%      split_pattern(Rest, [Bef|Acc]);
%%    nomatch ->
%%      lists:reverse([B|Acc])
%%  end.
%%
