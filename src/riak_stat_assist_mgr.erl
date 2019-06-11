%%%-------------------------------------------------------------------
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%%
%%% June 2019 update ->
%%%   Long winded functions
%%%   Functions used by many mngr modules
%%%   Commonly used functions are consolidated in this module.
%%%
%%% @end
%%% Created : 05. Jun 2019 13:31
%%%-------------------------------------------------------------------
-module(riak_stat_assist_mgr).

%% TODO: generalise the functions below, so they can be used for both
%% TODO: metadata and exometer.

%% TODO: Modernise the code, pull out outdated functions and replace with
%% TODO: quicker more generalised functions.

%% API
-export([find_entries/2, find_meta_entries/2]).

%%% ---- Stat show ---- %%%
-define(PFX, fun riak_stat_mngr:prefix/0).

find_entries(Arg, ToStatus) ->
  lists:map(
    fun(A) ->
      {S, Type, Status, DPs} = type_status_and_dps(A, ToStatus),
      case S of
        "[" ++ _ ->
          {find_entries_1(S, Type, Status), DPs};
        _ ->
          case legacy_search(S, Type, Status) of
            false ->
              {find_entries_1(S, Type, Status), DPs};
            Found ->
              {Found, DPs}
          end
      end
    end, Arg).

  %% TODO: finish writing this function
find_meta_entries(Arg, ToStatus) ->
  lists:map(
    fun(A) ->
      {S, Type, Status, DPs} = type_status_and_dps(A, ToStatus),
      case S of
        "[" ++ _ ->
          {find_entries_2(S, Type, Status), DPs}
      end end, Arg).

type_status_and_dps(S, ToStatus) ->
  [S1|Rest] = re:split(S, "/"),
  {Type, Status, DPs} = type_status_and_dps(Rest, '_', ToStatus, default),
  {S1, Type, Status, DPs}.

type_status_and_dps([<<"type=", T/binary>>|Rest], _Type, ToStatus, DPs) ->
  NewType = case T of
              <<"*">> -> '_';
              _ ->
                try binary_to_existing_atom(T, latin1)
                catch error:_ -> T
                end
            end,
  type_status_and_dps(Rest, NewType, ToStatus, DPs);
type_status_and_dps([<<"status=", St/binary>>|Rest], Type, _Status, DPs) ->
  NewStatus = case St of
                <<"enabled">>  -> enabled;
                <<"disabled">> -> disabled;
                <<"*">>        -> '_'
              end,
  type_status_and_dps(Rest, Type, NewStatus, DPs);
type_status_and_dps([DPsBin|Rest], Type, Status, DPs) ->
  NewDPs = merge([binary_to_existing_atom(D,latin1)
    || D <- re:split(DPsBin, ",")], DPs),
  type_status_and_dps(Rest, Type, Status, NewDPs);
type_status_and_dps([], Type, Status, DPs) ->
  {Type, Status, DPs}.

merge([_|_] = DPs, default) ->
  DPs;
merge([H|T], DPs) ->
  case lists:member(H, DPs) of
    true  -> merge(T, DPs);
    false -> merge(T, DPs ++ [H])
  end;
merge([], DPs) ->
  DPs.

find_entries_1(S, Type, Status) ->
  Patterns = lists:flatten([parse_stat_entry(S, Type, Status)]),
  riak_stat_exom_mgr:show_stat(Patterns).


%% TODO: change the parse for metadata to search the metadata uniquely
find_entries_2(S, Type, Status) ->
  Patterns = lists:flatten([parse_stat_entry(S, Type, Status)]),
  riak_stat_meta_mgr:show_stat(Patterns).

parse_stat_entry([], Type, Status) ->
  {{[riak_stat_mngr:prefix()] ++ '_', Type, '_'}, [{'=:=','$status',Status}], ['$_']};
parse_stat_entry("*", Type, Status) ->
  parse_stat_entry([], Type, Status);
parse_stat_entry("[" ++ _ = Expr, _Type, _Status) ->
  case erl_scan:string(ensure_trailing_dot(Expr)) of
    {ok, Toks, _} ->
      case erl_parse:parse_exprs(Toks) of
        {ok, [Abst]} ->
          partial_eval(Abst);
        Error ->
          io:fwrite("(Parse error for ~p: ~p~n", [Expr, Error]),
          []
      end;
    ScanErr ->
      io:fwrite("(Scan error for ~p: ~p~n", [Expr, ScanErr]),
      []
  end;
parse_stat_entry(Str, Type, Status) when Status==enabled; Status==disabled ->
  Parts = re:split(Str, "\\.", [{return,list}]),
  Heads = replace_parts(Parts),
  [{{H,Type,Status}, [], ['$_']} || H <- Heads];
parse_stat_entry(Str, Type, '_') ->
  Parts = re:split(Str, "\\.", [{return,list}]),
  Heads = replace_parts(Parts),
  [{{H,Type,'_'}, [], ['$_']} || H <- Heads];
parse_stat_entry(_, _, Status) ->
  io:fwrite("(Illegal status: ~p~n", [Status]).

ensure_trailing_dot(Str) ->
  case lists:reverse(Str) of
    "." ++ _ ->
      Str;
    _ ->
      Str ++ "."
  end.

partial_eval({cons,_,H,T}) ->
  [partial_eval(H) | partial_eval(T)];
partial_eval({tuple,_,Elems}) ->
  list_to_tuple([partial_eval(E) || E <- Elems]);
partial_eval({op,_,'++',L1,L2}) ->
  partial_eval(L1) ++ partial_eval(L2);
partial_eval(X) ->
  erl_parse:normalise(X).

replace_parts(Parts) ->
  case split("**", Parts) of
    {_, []} ->
      [replace_parts_1(Parts)];
    {Before, After} ->
      Head = replace_parts_1(Before),
      Tail = replace_parts_1(After),
      [Head ++ Pad ++ Tail || Pad <- pads()]
  end.

split(X, L) ->
  split(L, X, []).

split([H|T], H, Acc) ->
  {lists:reverse(Acc), T};
split([H|T], X, Acc) ->
  split(T, X, [H|Acc]);
split([], _, Acc) ->
  {lists:reverse(Acc), []}.

replace_parts_1([H|T]) ->
  R = replace_part(H),
  case T of
    ["**"] -> [R] ++ '_';
    _ -> [R|replace_parts_1(T)]
  end;
replace_parts_1([]) ->
  [].

replace_part(H) ->
  case H of
    "*" -> '_';
    "'" ++ _ ->
      case erl_scan:string(H) of
        {ok, [{atom, _, A}], _} ->
          A;
        Error ->
          error(Error)
      end;
    [C|_] when C >= $0, C =< $9 ->
      try list_to_integer(H)
      catch
        error:_ -> list_to_atom(H)
      end;
    _ -> list_to_atom(H)
  end.

pads() ->
  [['_'],
    ['_','_'],
    ['_','_','_'],
    ['_','_','_','_'],
    ['_','_','_','_','_'],
    ['_','_','_','_','_','_'],
    ['_','_','_','_','_','_','_'],
    ['_','_','_','_','_','_','_','_'],
    ['_','_','_','_','_','_','_','_','_'],
    ['_','_','_','_','_','_','_','_','_','_'],
    ['_','_','_','_','_','_','_','_','_','_','_'],
    ['_','_','_','_','_','_','_','_','_','_','_','_'],
    ['_','_','_','_','_','_','_','_','_','_','_','_','_'],
    ['_','_','_','_','_','_','_','_','_','_','_','_','_','_'],
    ['_','_','_','_','_','_','_','_','_','_','_','_','_','_','_'],
    ['_','_','_','_','_','_','_','_','_','_','_','_','_','_','_','_']].

legacy_search(S, Type, Status) ->
  case re:run(S, "\\.", []) of
    {match,_} ->
      false;
    nomatch ->
      Re = <<"^", (make_re(S))/binary, "$">>,
      [{S, legacy_search_1(Re, Type, Status)}]
  end.

make_re(S) ->
  repl(split_pattern(S, [])).

legacy_search_1(N, Type, Status) ->
  Found = riak_stat_mngr:aliases(regexp_foldr, [N]),
  lists:foldr(
    fun({Entry, DPs}, Acc) ->
      case match_type(Entry, Type) of
        true ->
          DPnames = [D || {D,_} <- DPs],
          case riak_stat_mngr:get_stat(Entry, DPnames) of
            {ok, Values} when is_list(Values) ->
              [{Entry, zip_values(Values, DPs)} | Acc];
            {ok, disabled} when Status=='_';
              Status==disabled ->
              [{Entry, zip_disabled(DPs)} | Acc];
            _ ->
              [{Entry, [{D,undefined} || D <- DPnames]}|Acc]
          end;
        false ->
          Acc
      end
    end, [], orddict:to_list(Found)).

match_type(_, '_') ->
  true;
match_type(Name, T) ->
  T == riak_stat_mngr:info(Name, type).

zip_values([{D,V}|T], DPs) ->
  {_,N} = lists:keyfind(D, 1, DPs),
  [{D,V,N}|zip_values(T, DPs)];
zip_values([], _) ->
  [].

zip_disabled(DPs) ->
  [{D,disabled,N} || {D,N} <- DPs].

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

%% TODO: move the internal funcitons for stat info into this module

%% TODO: move the internal functions for stat enable/disable into this

%% TODO: move the internal functions for stat reset into this module

%% TODO: create functions for stat deletion.

%% TODO: create a function for setting defaults on metadata and exometer?
%% as in the lww and allow_multi, true sort of thing.

%% TODO: implement a function that will automatically persist data from
%% exometer into the metadata. include functionality such as once a week/
%% month/3months etc...?

%% There are stats that are to do with the system info, puts and gets times,
%% vnode data. there could be stats that are to do with the run time or anythin.


%%% ---- Stat compare ---- %%%

%%% compare(stat1, stat2) ->
%%% compares the difference in stats, one will be from exometer, the other from
%%% the metadata, if method == lww then it will check the vector clocks and
%%% it will replace the data on the oldest one. basically returns the newest data
%%% and then updates both the metadata and the exometer data with that value.


%%% compare_fix(stat1, stat2) ->
%%% if lww, then it replaces.
%%% compare_fix(stat1, stat2) ->
%%% if metadata then the stat from metadata will replace.
%%% compare_fix(stat1, stat2) ->
%%% if exometer then the stat from exometer will replace.

%% TODO: write a function that compares the data (status) in exom and meta

%%% -----

%%change_helper(Mod, Stats, St) ->
%%  lists:foreach(fun(N) ->
%%    change_status(Mod, N, St)
%%                end, Stats).

% change_status(Mod, N, St) ->
%   Value = check_meta(M, N, St),
%   EVal  = check_exom(M, N),
%   {_,Return} = riak_stat_assist_mgr:stat_compare(Value, Eval)
%   Return.
