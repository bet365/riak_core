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
%%%
%%%   Moved everything from Folsom over to exometer
%%%
%%% @end
%%% Created : 05. Jun 2019 16:17
%%%-------------------------------------------------------------------
-module(riak_stat_exom_mgr).
-author("savannahallsop").

%% TODO:
%% make sure everything returns ok | answer | {error, Reason}

-export([
  register_stat/4, re_register/2, re_register/3, alias/1, aliases/2,
  get_values/1, get_datapoint/2, get_value/1, select_stat/1, info/2,
  update_or_create/3, update_or_create/4, set_opts/2,
  unregister_stat/1, reset_stat/1, timestamp/0]).

-export([start/0, stop/0]).

-export([get_datapoints/2, get_datapoints/3, get_value_folsom/4, notify_metric/3,
  delete_metric/1, get_metric_value/1,new_counter/1, new_history/2, new_gauge/1]).

-define(PFX, riak_stat_mngr:prefix()).
-define(TS, timestamp()).

%%%-------------------------------------------------------------------
%%% API
%%%-------------------------------------------------------------------

%%%%%%%%%%%% REGISTERING %%%%%%%%%%%%%

-spec(register_stat(StatName :: list(), Type :: atom(), Opts :: list(), Aliases :: term()) ->
  ok | {error, Reason :: term()}).
%% @doc
%% Registers all stats, using exometer:re_register/3, any stat that is
%% re_registered overwrites the previous entry, works the same as
%% exometer:new/3 except it will ont return an error if the stat already
%% is registered.
%% @end
register_stat(StatName, Type, Opts, Aliases) ->
  re_register(StatName, Type, Opts), %% returns -> ok.
  lists:foreach(
    fun({DP, Alias}) ->
      aliases(new, [Alias, StatName, DP]) %% returns -> ok | {error, Reason}
    end, Aliases).

re_register(StatName, Type) ->
  re_register(StatName, Type, []).

re_register(StatName, Type, Opts) ->
  exometer:re_register(StatName, Type, Opts).

-spec(alias(Group :: term()) -> ok | term()).
alias(Group) ->
  lists:keysort(
    1,
    lists:foldl(
      fun({K, DPs}, Acc) ->
        case get_datapoint(K, [D || {D,_} <- DPs]) of
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

-spec(aliases(Type :: atom(), Entry :: term()) -> ok | term()).
%% @doc
%% goes to exometer_alias and performs the type of alias function specified
%% @end
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

-spec(get_value(Stat :: list()) -> ok | term()).
%% @doc
%% Same as the function above, except in exometer the Datapoint:
%% 'default' is inputted, however it is used by some modules
%% @end
get_value(S) ->
  exometer:get_value(S).

-spec(select_stat(Pattern :: term()) -> term()).
%% @doc
%% Find the stat in exometer using this pattern
%% @end
select_stat(Pattern) ->
  exometer:select(Pattern).

-spec(info(Name :: list() | term(), Type :: atom() | term()) -> term()).
%% @doc
%% find information about a stat on a specific item
%% @end
info(Name, Type) ->
  exometer:info(Name, Type).

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

-spec(set_opts(StatName :: list() | atom(), Opts :: list()) -> ok | term()).
%% @doc
%% Set the options for a stat in exometer, setting the status as either enabled or
%% disabled in it's options in exometers will change its status in the entry
%% @end
set_opts(StatName, Opts) ->
  exometer:setopts(StatName, Opts).

%%%%%%%%%%%%% UNREGISTER / RESET %%%%%%%%%%%%%%

-spec(unregister_stat(StatName :: term()) -> ok | term() | {error, Reason :: term()}).
%% @doc
%% deletes the stat entry from exometer
%% @end
unregister_stat(StatName) ->
  exometer:delete(StatName).

-spec(reset_stat(StatName :: term()) -> ok | term()).
%% @doc
%% resets the stat in exometer
%% @end
reset_stat(StatName) ->
  exometer:reset(StatName).

-spec(timestamp() -> term()).
%% @doc
%% Returns the timestamp to put in the stat entry
%% @end
timestamp() ->
  exometer_util:timestamp().

%%%%%%%% Testing %%%%%%%%

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





