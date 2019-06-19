%%%-------------------------------------------------------------------
%%% @copyright (C) 2019, Bet365
%%% @doc
%%% Bouncer for information heading to exometer.
%%%
%%% June 2019 ->
%%%   Got rid of the exometer:new functionality and redirected the
%%%   functions in riak_kv_stat to use re_register, as new/3 acts
%%%   very similar to re_register, and the use in riak_kv_stat is that
%%%   of re_registering anyway and only in 2 places.
%%%
%%%   removed the exometer:update/2 functionality and replaced it with
%%%   the update_or_create function as a default.
%%% @end
%%% Created : 05. Jun 2019 16:17
%%%-------------------------------------------------------------------
-module(riak_stat_exom_mgr).
-author("savannahallsop").

-export([
  register_stat/4, get_values/1, get_datapoint/2,
  update_or_create/3, update_or_create/4, unregister_stat/1]).

%% API
-export([find_entries/1, find_entries/2, show_stat/1]).

-export([start/0, stop/0,
  info/2, get_value/1,
  re_register/2, re_register/3,
    alias/1, aliases/2,
   select/1,
  reset/1,
  delete/1,
  delete_metric/1, get_metric_value/1,new_counter/1, new_history/2, new_gauge/1]).

-export([get_datapoints/2, get_datapoints/3, get_value_folsom/4, notify_metric/3]).

-define(PFX, fun riak_stat_mngr:prefix/0).


%%%%%%%%%%%%% DOCUMENTATION %%%%%%%%%%%%%%



%%%-------------------------------------------------------------------
%%% API
%%%-------------------------------------------------------------------

%%%%%%%%%%%% REGISTERING %%%%%%%%%%%%%

% register the stat in exometer
-spec(register_stat(StatName :: list(), Type :: atom(), Opts :: list(), Aliases :: term()) ->
  ok | term() | {error, Reason}).
%% @doc
%% Registers all stats, using exometer:re_register/3, any stat that is
%% re_registered overwrites the previous entry, works the same as
%% exometer:new/3 except it will ont return an error if the stat already
%% is registered.
%% @end
register_stat(StatName, Type, Opts, Aliases) ->
  re_register(StatName, Type, Opts),
  lager:info("Stat registered in exometer: {~p,~p,~p,~p}~n", [StatName, Type, Opts, Aliases]),
  lists:foreach(
    fun({DP, Alias}) ->
      aliases(new, [Alias, StatName, DP])
    end, Aliases).

re_register(StatName, Type) ->
  re_register(StatName, Type, []).

re_register(StatName, Type, Opts) ->
  exometer:re_register(StatName, Type, Opts).

aliases(new, [Alias, StatName, DP]) ->
  exometer_alias:new(Alias, StatName, DP);
aliases(prefix_foldl, []) ->
  exometer_alias:prefix_foldl(<<>>, alias_fun(), orddict:new());
aliases(regexp_foldr, [N]) ->
  exometer_alias:regexp_foldr(N, alias_fun(), orddict:new()).


%%%%%%%%%%%%% READING DATA %%%%%%%%%%%%

-spec(get_values(Path :: any()) -> term()).
%% @doc
%% The Path is the start or full name of the stat(s) you wish to find,
%% i.e. [riak,riak_kv] as a path will return stats with those to elements
%% in their path.
%% @end
get_values(Path) ->
  exometer:get_values(Path).

-spec(get_datapoint(Name :: term(), Datapoint :: term()) -> term()).
%% @doc
%% Retrieves the datapoint value from exometer
%% @end
get_datapoint(Name, Datapoint) ->
  exometer:get_value(Name, Datapoint).



%%%%%%%%%%%%%% UPDATING %%%%%%%%%%%%%%

-spec(update_or_create(Name :: term(), UpdateVal :: any(), Type :: atom() | term()) ->
  ok | term()).
%% @doc
%% Sends the stat to exometer to get updated, unless it is not already a stat then it
%% will be created. First it is checked in meta_mgr and registered there.
%% @end
update_or_create(Name, UpdateVal, Type) ->
  update_or_create(Name, UpdateVal, Type, []).
-spec(update_or_create(Name :: list() | atom(), UpdateVal :: any(), Type :: atom() | term(), Opts :: list()) ->
  ok | term()).
update_or_create(Name, UpdateVal, Type, Opts) ->
  exometer:update_or_create(Name, UpdateVal, Type, Opts).

%%%%%%%%%%%%% UNREGISTER %%%%%%%%%%%%%%

-spec(unregister_stat(StatName :: term()) -> ok | term() | {error, Reason}).
%% @doc
%% deletes the stat entry from exometer
%% @end
unregister_stat(StatName) ->
  exometer:delete(StatName).












%TODO: change metrics from folsom to just exometer core

% TODO: update helper functions





find_entries(Arg) ->
  find_entries(Arg, enabled).

find_entries(Arg, ToStatus) ->
  riak_stat_assist_mgr:find_entries(Arg, ToStatus).










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


%Todo: spec




%todo: spec

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





