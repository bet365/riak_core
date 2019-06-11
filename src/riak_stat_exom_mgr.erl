%%%-------------------------------------------------------------------
%%% @copyright (C) 2019, Bet365
%%% @doc
%%% Bouncer for information heading to exometer.
%%%
%%% @end
%%% Created : 05. Jun 2019 16:17
%%%-------------------------------------------------------------------
-module(riak_stat_exom_mgr).
-author("savannahallsop").

%% API
-export([find_entries/1, find_entries/2, show_stat/1]).

-export([start/0, stop/0,
  info/2, get_value/1, get_value/2,
  get_values/1, re_register/2, re_register/3,
  register_stat/2,
  alias/1, aliases/2,
  ensure/3, select/1,
  reset/1,
  delete/1,
  delete_metric/1, get_metric_value/1,
  update/2,
  update_or_create/3, new_counter/1, new_history/2, new_gauge/1]).

-export([get_datapoints/2, get_datapoints/3, get_value_folsom/4, notify_metric/3]).

-define(PFX, fun riak_stat_mngr:prefix/0).

%%%-------------------------------------------------------------------
%%% API
%%%-------------------------------------------------------------------



% TODO: merge functions,
% TODO: change metrics from folsom to just exometer core

% TODO: update helper functions





find_entries(Arg) ->
  find_entries(Arg, enabled).

find_entries(Arg, ToStatus) ->
  riak_stat_assist_mgr:find_entries(Arg, ToStatus).



re_register(StatName, Type) ->
  re_register(StatName, Type, []).

re_register(StatName, Type, Opts) ->
  exometer:re_register(StatName, Type, Opts).

register_stat(Name, Type) ->
  exometer:new(Name, Type).


update(Arg, Value) ->
  exometer:update(Arg, Value).
%% TODO: give explanation for why a different update and update_or_create
update_or_create(Name, UpdateVal, Type) ->
  exometer:update_or_create(Name, UpdateVal, Type, []).




% TODO: SPec
show_stat(Pats) ->
  exometer:select(Pats).

%% TODO: all the functionality needed in the assistant manager will call into this module
%%      it can be transformed in this module before it reaches exometer.













% TODO: Spec
info(Name, Type) ->
  exometer:info(Name, Type).

%TODO: spec
get_value(S) ->
  exometer:get_value(S).
%TODO: spec
get_value(N, DP) ->
  exometer:get_value(N, DP).

%Todo: spec
get_values(Path) ->
  exometer:get_values(Path).



%todo: spec
aliases(new, [Alias, StatName, DP]) ->
  exometer_alias:new(Alias, StatName, DP);
aliases(prefix_foldl, []) ->
  exometer_alias:prefix_foldl(<<>>, alias_fun(), orddict:new());
aliases(regexp_foldr, [N]) ->
  exometer_alias:regexp_foldr(N, alias_fun(), orddict:new()).

% TODO SPEC
alias(Group) ->
  lists:keysort(
    1,
    lists:foldl(
      fun({K, DPs}, Acc) ->
        case get_value(K, [D || {D,_} <- DPs]) of
          {ok, Vs} when is_list(Vs) ->
            lists:foldr(fun({D,V}, Acc1) ->
              {_,N} = lists:keyfind(D,1,DPs),
              [{N,V}|Acc1]
                        end, Acc, Vs);
          Other ->
            Val = case Other of
                    {ok, disabled} -> undefined;
                    _ -> 0
                  end,
            lists:foldr(fun({_,N}, Acc1) ->
              [{N,Val}|Acc1]
                        end, Acc, DPs)
        end
      end, [], orddict:to_list(Group))).

%TODO SPec
ensure(Stat, Type, Alias) ->
  exometer:ensure(Stat, Type, Alias).

select(Pats) ->
  exometer:select(Pats).

% TODO: Spec
reset(Name) ->
  exometer:reset(Name).

%TOdO SPEc
delete(Arg) ->
  exometer:delete(Arg).



%% Testing
start() ->
  exometer:start().

stop() ->
  exometer:stop().


%%%-------------------------------------------------------------------
%%% exometer - folsom
%%%-------------------------------------------------------------------


%% TODO: move everything towards exometer, remove all the folsom functions


get_datapoints(Name, Type) ->
  get_datapoints(Name, Type, []).
get_datapoints(Name, Type, _Opts) ->
  exometer_folsom:get_datapoints(Name, Type, []).

get_value_folsom(Name, Type, _Opts, DPs) ->
  exometer_folsom:get_value(Name, Type, [], DPs).

notify_metric(Name, Value, Type) ->
  folsom_metrics:notify_existing_metric(Name, Value, Type).

get_metric_value(Arg) ->
  folsom_metrics:get_metric_value(Arg).

delete_metric(Arg) ->
  folsom_metrics:delete_metric(Arg).

new_counter(Arg) ->
  folsom_metrics:new_counter(Arg).

new_history(Name, Arg) ->
  folsom_metrics:new_history(Name, Arg).

new_gauge(Arg) ->
  folsom_metrics:new_gauge(Arg).


%%%-------------------------------------------------------------------
%%% Helper functions
%%%-------------------------------------------------------------------


alias_fun() ->
  fun(Alias, Entry, DP, Acc) ->
    orddict:append(Entry, {DP, Alias}, Acc)
  end.





