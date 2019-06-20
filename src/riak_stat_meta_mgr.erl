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

-define(PFX, riak_stat_mngr:prefix()).
-define(STAT, stats).
-define(NODEID, riak_core_nodeid:get()).

%%%-------------------------------------------------------------------
%%% API
%%%-------------------------------------------------------------------

%%%%%%%%%%%% REGISTERING %%%%%%%%%%%%

% check if the stat is already registered in metadata, and pull out the
% options
-spec(register_stat(StatName :: metadata_key(), Type :: atom() | term(), Opts :: list(), Aliases :: term()) ->
  ok | term() | {error, Reason}).
%% @doc
%% Checks if the stat is already registered in the metadata, if not it
%% registers it, and pulls out the options for the status and sends it
%% back to go into exometer
%% @end
register_stat(StatName, Type, Opts, Aliases) ->
  case check_meta(StatName) of % check registration
    undefined -> % if not registered return default Opts
      re_register_stat(StatName, Type, [{vclock, vclock:fresh(?NODEID, 1)}| Opts], Aliases),
      Opts;
    {_Type, MetaOpts, _Aliases} -> % if registered
      find_register_status(Opts, MetaOpts);
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
      case find_unregister_status(Value) of
        true ->
          lager:debug("Stat is unregistered: ~p~n", [StatName]),
        unregistered; % todo: io:fwrite?
        false ->
          Value
      end;
    _ ->
      lager:debug("riak_stat_meta_mgr:check_meta -- Could not check_meta~n")
  end.

find_register_status(NewOpts, MetaOpts) ->
  case lists:keyfind(status, 1, MetaOpts) of
    false ->
      NewOpts;
    Status -> % {status, disabled}
      [Status | NewOpts];
    _ ->
      lager:debug("riak_stat_meta_mgr:find_register_status -- neither 'false' nor Status~n")
  end.

find_unregister_status({{_NI, _S}, SN, {_T, Opts, _A}}) ->
  case lists:keyfind(unregistered, 1, Opts) of
    false ->
      set_options(SN, {unregistered, false}),
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
      set_options(Statname, Type, NewOpts, Aliases)
  end.

%%%%%%%%%% READING OPTS %%%%%%%%%%%%

-spec(check_status(StatName :: metadata_key()) -> term()).
%% @doc
%% Returns the status of the stat saved in the metadata
%% @end
check_status(StatName) ->
  case check_meta(StatName) of
    {{_NI, _S}, StatName, {_T, Opts, _A}} ->
      Status = find_register_status([], Opts),
      io:fwrite("~p:~p~n", [StatName, Status]);
    Reason ->
      {error, Reason}
  end.

%%%%%%%%%% SET OPTIONS %%%%%%%%%%%%%

-spec(change_status(Statname :: metadata_key(), ToStatus :: atom()) -> ok | term()).
%% @doc
%% Changes the status in the metadata
%% @end
change_status(Statname, ToStatus) ->
  set_options(Statname, {status, ToStatus}).

-spec(set_options(StatName :: metadata_key(), NewOpts :: list() | tuple()) ->
  ok | term() | {error, Reason}).
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
  {value, {vclock, [{Node, {Count, _VC}}]}} = lists:keysearch(vclock, 1, Opts),
  lists:keyreplace(vclock, 1, Opts, {vclock, clock_fresh(Node, Count)}).

reset_inc(Count) -> Count + 1.

clock_fresh(Node, Count) ->
  vclock:fresh(Node, vc_inc(Count)).
vc_inc(Count) -> Count + 1.


%%%%%%%%% PROFILES %%%%%%%%%%

load_profile(ProfileName) ->
  ok.

reset_profile() ->
  % enable all stats
  ok.

add_profile(ProfileName, Stats) ->
  ok.

add_profile_stat(ProfileName, StatName) ->
  ok.

remove_profile(ProfileName) ->
  ok.

remove_profile_stat(ProfileName, Stat) ->
  ok.

change_profile_stat(ProfileName, Stat) ->
  % change the status to be the opposite
  ok.

check_profile_stat(ProfileName, Stat) ->
   ok.
