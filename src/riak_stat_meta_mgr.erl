%%%-------------------------------------------------------------------
%%% @copyright (C) 2019, Bet365
%%% @doc
%%% Middle-man for the stats/consoles and exometer/metadata
%%% @end
%%% Created : 04. Jun 2019 11:39
%%%-------------------------------------------------------------------
-module(riak_stat_meta_mgr).

-include("riak_core_metadata.hrl").

-export([
  register_stat/4, unregister/1, check_meta/1,
  check_status/1, change_status/2,
  set_options/2, reset_stat/1]).

-export([load_profile/1, reset_profile/1, add_profile/2,
  add_profile_stat/2, remove_profile/1, remove_profile_stat/2,
  change_profile_stat/2, check_profile_stat/2]).

-define(PFX, riak_stat_mngr:prefix()).
-define(STAT, stats).
-define(PROF, profiles).
-define(NODEID, riak_core_nodeid:get()).
% TODO: add vclock as macro

%%%-------------------------------------------------------------------
%%% API
%%%-------------------------------------------------------------------

%%%%%%%%%%%% REGISTERING %%%%%%%%%%%%

% check if the stat is already registered in metadata, and pull out the
% options
-spec(register_stat(StatName :: metadata_key(), Type :: atom() | term(), Opts :: list(), Aliases :: term()) ->
  ok | term() | {error, Reason :: term()}).
%% @doc
%% Checks if the stat is already registered in the metadata, if not it
%% registers it, and pulls out the options for the status and sends it
%% back to go into exometer
%% @end
register_stat(StatName, Type, Opts, Aliases) ->
  case check_meta(StatName) of % check registration
    undefined -> % if not registered return default Opts
      re_register_stat(StatName, Type, [{vclock, vclock:fresh(?NODEID, 1)} | Opts], Aliases),
      Opts;
    {_Type, MetaOpts, _Aliases} -> % if registered
      find_register_status(Opts, MetaOpts);
%%      re_register_stat(StatName, Type, NewOpts, Aliases);
    _ ->
      lager:debug("riak_stat_meta_mgr:register_stat --
      Could not register the stat:~n{{~p,~p},~p,{~p,~p,~p}}~n",
        [?NODEID, ?STAT, StatName, Type, Opts, Aliases])
  end.

-spec(check_meta(StatName :: metadata_key()) -> ok | term()).
%% @doc
%% returns the value of the stat from the metadata
%% @end
check_meta(StatName) ->
  case riak_core_metadata:get({?NODEID, ?STAT}, StatName) of
    undefined ->
      undefined;
    [] ->
      undefined;
    Value ->
      case find_unregister_status(StatName, Value) of
        true ->
          lager:debug("Stat is unregistered: ~p~n", [StatName]),
          unregistered;
        false ->
          Value
      end
  end.

find_register_status(NewOpts, MetaOpts) ->
  case lists:keyfind(status, 1, MetaOpts) of
    false ->
      NewOpts;
    Status -> % {status, disabled}
      [Status | NewOpts]
  end.

find_unregister_status(_SN, {_T, Opts, _A}) ->
  case lists:keyfind(unregistered, 1, Opts) of
    false ->
%%      set_options(SN, {unregistered, false}),
      false;
    {unregistered, Bool} ->
      Bool
  end.

re_register_stat(StatName, Type, Opts, Aliases) ->
  % {{NodeId, stats}, [riak, riak_kv, node, gets], {spiral, [{resets,1},{status,enabled}],...}}
  riak_core_metadata:put({?NODEID, ?STAT}, StatName, {Type, Opts, Aliases}),
%%  lager:info("Stat registered in metadata: {{~p,~p},~p,{~p,~p,~p}}~n"),
  ok.


%%%%%%%%%% UNREGISTERING %%%%%%%%%%%%

-spec(unregister(StatName :: term()) -> ok | term()).
%% @doc
%% Marks the stats as unregistered, that way when a node is restarted and registers the
%% stats it will ignore stats that are marked unregistered
%% @end
unregister(Statname) ->
  case check_meta(Statname) of
    unregistered ->
      lager:info("Stat is unregistered: ~p~n", [Statname]),
      unregistered;
    {Type, MetaOpts, Aliases} ->
      NewOpts = lists:keyreplace(unregistered, 1, MetaOpts, {unregistered, true}),
      set_options(Statname, Type, NewOpts, Aliases),
      ok;
    _ ->
      ok
  end.

%%%%%%%%%% READING OPTS %%%%%%%%%%%%

-spec(check_status(StatName :: metadata_key()) -> term()).
%% @doc
%% Returns the status of the stat saved in the metadata
%% @end
check_status(StatName) ->
  case check_meta(StatName) of
    {{_NI, _S}, StatName, {_T, Opts, _A}} ->
      find_register_status([], Opts);
%%      io:fwrite("~p:~p~n", [StatName, Status]);
    Reason ->
      {error, Reason}
  end.

%%%%%%%%%% SET OPTIONS %%%%%%%%%%%%%

-spec(change_status(Statname :: metadata_key(), ToStatus :: atom()) -> ok | term()).
%% @doc
%% Changes the status in the metadata
%% @end
change_status(Statname, ToStatus) ->
  case check_status(Statname) of
    DiffStatus when DiffStatus =/= {status, ToStatus} ->
      set_options(Statname, {status, ToStatus});
    DiffStatus when DiffStatus == ToStatus ->
      {error, no_change}
  end.
%%  set_options(Statname, {status, ToStatus}).

-spec(set_options(StatName :: metadata_key(), NewOpts :: list() | tuple()) ->
  ok | term() | {error, Reason :: term()}).
%% @doc
%% Setting the options in the metadata manually, such as {unregistered, false | true} etc...
%% @end
set_options(Statname, NewOpts) when is_list(NewOpts) ->
  lists:foreach(fun({Key, NewVal}) ->
    set_options(Statname, {Key, NewVal})
                end, NewOpts);
set_options(Statname, {Key, NewVal}) ->
  case check_meta(Statname) of
    undefined ->
      io:fwrite("Stat is not registered: ~p~n", [Statname]);
    unregistered ->
      io:fwrite("Stat is unregistered: ~p~n", [Statname]);
    {Type, Opts, Aliases} ->
      NewOpts = lists:keyreplace(Key, 1, Opts, {Key, NewVal}),
      NewOpts2 = fresh_clock(NewOpts),
      set_options(Statname, Type, NewOpts2, Aliases)
  end.

set_options(StatName, Type, NewOpts, Aliases) ->
  re_register_stat(StatName, Type, NewOpts, Aliases).

%%%%%%%%% RESETTING %%%%%%%%%%%

-spec(reset_stat(Statname :: metadata_key()) -> ok | term()).
%% @doc
%% reset the stat in exometer and notify exometer of its reset
%% @end
reset_stat(Statname) ->
  case check_meta(Statname) of
    undefined ->
      io:fwrite("Stat is not registered: ~p~n", [Statname]);
    unregistered ->
      io:fwrite("Stat is unregistered: ~p~n", [Statname]);
    {Type, Opts, Aliases} ->
      {value, {resets, Resets}} = lists:keysearch(resets, 1, Opts),
      NewOpts1 = change_status(Statname, enabled),
      set_options(Statname, Type,
        lists:keyreplace(resets, 1, NewOpts1, {resets, reset_inc(Resets)}), Aliases)
  end.

fresh_clock(Opts) ->
  case lists:keysearch(vclock, 1, Opts) of
    false ->
      [{vclock, clock_fresh(?NODEID, 0)} | Opts];
    {value, {vclock, [{Node, {Count, _VC}}]}} ->
      lists:keyreplace(vclock, 1, Opts, {vclock, clock_fresh(Node, Count)});
    _ ->
      [{vclock, clock_fresh(?NODEID, 0)} | Opts]
  end.


reset_inc(Count) -> Count + 1.

clock_fresh(Node, Count) ->
  vclock:fresh(Node, vc_inc(Count)).
vc_inc(Count) -> Count + 1.


%%%%%%%%% PROFILES %%%%%%%%%%


load_profile(ProfileName) ->
  case check_profile_stat(ProfileName, '_') of
    {error, Reason} ->
      io:fwrite("error: ~p~n", [Reason]);
    Stats ->
      change_these_stats(Stats)
  end.

-spec(reset_profile(ProfileName :: term()) -> ok | term()).
%% @doc
%% takes the stats that are disabled out of the metadata and returns them to
%% the stat_mngr so they can be re-enabled, the profile is then removed
reset_profile(ProfileName) ->
  % return the disabled stats
  check_profile_stat(ProfileName, {'_', {status, disabled}}).

-spec(add_profile(ProfileName :: term(), Stats :: list()) -> ok | term()).
%% @doc
%% add a profile into the metadata with a list of stats as its value, inside
%% the stats value is the status of said stat, upon loading of the profile the
%% status is checked and changed in exometer
%% @end
add_profile(ProfileName, Stats) ->
  register_profile(ProfileName, Stats).

-spec(add_profile_stat(ProfileName :: term(), Stats :: term()) -> ok | term()).
%% @doc
%% Adds stat(s) to the profile in the metadata, compares the lists of stats
%% going in to the stats that are already in the metadata,
%% acts as lww with the added stats taking over
%% @end
add_profile_stat(ProfileName, Stats) ->
  add_stat(ProfileName, Stats).

-spec(remove_profile(ProfileName :: term()) -> term()).
%% @doc
%% remove the profile from the metadata - the metadata doesn't delete
%% any actual data, it leaves behind a tombstone
%% @end
remove_profile(ProfileName) ->
  tombstone_profile(ProfileName).

-spec(remove_profile_stat(ProfileName :: term(), Stat :: term()) -> term()).
%% @doc
%% removes stat(s) from the profiles metadata and automatically enables any that
%% are removed. Like an undo, it returns the stat back to default
%% @end
remove_profile_stat(ProfileName, Stat) ->
  stat_remover(ProfileName, Stat).

-spec(change_profile_stat(ProfileName :: term(), Stat :: term()) -> term()).
%% @doc
%% change the status of the current stat to the opposite
%% @end
change_profile_stat(ProfileName, Stat) ->
  change_stat_status(ProfileName, Stat).

-spec(check_profile_stat(ProfileName :: term(), Stats :: term()) ->
  ok | term() | {error, Reason :: term()}).
%% @doc
%% check in the metadata for a profile and the stat(s) entered
%% @end
check_profile_stat(ProfileName, Stats) ->
  check_prof_meta(ProfileName, Stats).


change_these_stats(Stats) ->
  riak_stat_mngr:change_these_stats(Stats).

register_profile(ProfileName, Stats) ->
  riak_core_metadata:put({riak_stat, ?PROF}, ProfileName, Stats),
  io:fwrite("Profile Stats stored in metadata ~n").

check_prof_meta(ProfileName, Stat) ->
  case riak_core_metadata:get({riak_stat, ?PROF}, ProfileName) of
    undefined ->
      undefined;
    [] ->
      undefined;
    Value ->
      case find_profile_stat_status(Value, Stat) of
        false ->
          [];
        Status ->
          [{Stat, Status}]
      end
  end.

find_profile_stat_status({{_RS, _PR}, _PN, Stats}, Stat) ->
  case lists:keyfind(Stat, 1, Stats) of
    false ->
      false;
    {Stat, {status, Status}} ->
      Status
  end.

change_stat_status(Profile, Stats) ->
  MetaStats = get_profile_stats(Profile),
  ChangeStatus =
    lists:foldl(fun(Stat, StatsAcc) ->
      case check_prof_meta(Profile, Stat) of
        [{Key, disabled}] ->
          [{Key, {status, enabled}} | StatsAcc];
        [{Key, enabled}] ->
          [{Key, {status, disabled}} | StatsAcc];
        _ ->
          StatsAcc
      end end, [], Stats),
  change_these_stats(ChangeStatus),
  FinalStats = the_alpha_stat(ChangeStatus, MetaStats),
  add_profile(Profile, FinalStats).

get_profile_stats(ProfileName) ->
  riak_core_metadata:get({riak_stat, ?PROF}, ProfileName).

add_stat(ProfileName, Stats) ->
  MetaStats = get_profile_stats(ProfileName),
  FinalStats = the_alpha_stat(Stats, meta_keyer(MetaStats)),
  change_these_stats(FinalStats),
  add_profile(ProfileName, FinalStats).

stat_remover(ProfileName, Stats) ->
  MetaStats = get_profile_stats(ProfileName),
  EnabledStats =
    lists:foldl(fun(Stat, EnabledAcc) ->
      case check_prof_meta(ProfileName, Stat) of
        [{Key, disabled}] ->
          [{Key, {status, enabled}} | EnabledAcc];
        _ ->
          EnabledAcc
      end end, [], Stats),
  change_these_stats(EnabledStats),
  FinalStats = the_alpha_stat(EnabledStats, MetaStats),
  add_profile(ProfileName, FinalStats).

the_alpha_stat(Stats1, Stats2) ->
  riak_stat_mngr:the_alpha_stat(Stats1, Stats2).

meta_keyer(Stats) ->
  lists:map(fun({Key, Value}) ->
    meta_keyer(Key, Value)
            end, Stats).

meta_keyer(Key, Value) ->
  riak_stat_mngr:meta_keyer(Key, Value).

tombstone_profile(Profile) ->
  riak_core_metadata:delete({riak_stat, ?PROF}, Profile).