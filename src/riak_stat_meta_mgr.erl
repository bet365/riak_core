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
  register_stat/4, unregister/1, check_status/1, set_options/2]).

%% API
-export([find_entries/1, find_entries/2]).


-define(PFX, fun riak_stat_mngr:prefix/0).
-define(STAT, stats).
-define(NODEID, fun riak_core_nodeid:get/0).

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
      re_register_stat(StatName, Type, Opts, Aliases),
      Opts;
    {_Type, MetaOpts, _Aliases} -> % if registered
      find_register_status(Opts, MetaOpts);
    _ ->
      lager:debug("riak_stat_meta_mgr:register_stat --
      Could not register the stat:~n{{~p,~p},~p,{~p,~p,~p}}~n",
        [?NODEID, ?STAT, StatName, Type, Opts, Aliases])
  end.

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
  lager:info("Stat registered in metadata: {{~p,~p},~p,{~p,~p,~p}}~n").


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

-spec(check_status(StatName :: term()) -> term()).
%% @doc
%% Returns the status of the stat saved in the metadata
%% @end
check_status(StatName) ->
  case check_meta(StatName) of
    {{_NI, _S}, StatName, {_T, Opts, _A}} ->
      Status = find_register_status([], Opts),
      riak_stat_assist_mgr:print_status(StatName, Status);
    Reason ->
      {error, Reason}
  end.

%%%%%%%%%% SET OPTIONS %%%%%%%%%%%%%

-spec(set_options(StatName :: atom() | list(), NewOpts :: list() | tuple()) ->
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
      set_options(Statname, Type, NewOpts, Aliases)
  end.

set_options(StatName, Type, NewOpts, Aliases) ->
  re_register_stat(StatName, Type, NewOpts, Aliases).

%%%%%%%%% RESETTING %%%%%%%%%%%













%%find_status(Statname) ->
%%  print_stat(check_meta(Statname)).


find_entries(Arg) ->
  find_entries(Arg, enabled).

find_entries(Arg, Status) ->
  riak_stat_assist_mgr:find_meta_entries(Arg, Status).

% TODO: actually write functions for the metadata
% TODO: abstract it all out
% TODO: below












reset_inc(Count) -> Count + 1.


%% check_stats(all) ->
%% check_stats([Stat]) -> % checks a list of stats given, riak.riak_kv.node.gets etc...
%%% taken from riak-admin, pass a list to console, translate to here

%% print_stats(all) -> % all that are enabled and disabled
%% print_stats(enabled) -> % print all stats that are enabled
%% print_stats(disabled) -> % print all stats that are disabled

%% disable_all(all) -> % typing in all the riak-admin disables all stats on all nodes
%% disable_all([Stats]) -> % disables these specific stats on all nodes
%% disable_stat_for_me(all) -> % disables all stats on one node
%% disable_stat_for_me([Stats]) -> % disables specific stats on one node
%% disable_stat_for_them(all) -> % disable all stats on every node except this one
%% disable_stat_for_them([Stats]) -> % disables specific stats on all nodes except this one.
%% the function below can be used to both enable and disable the stat.

%%%% Mod is the module stat value, it is currently set to core, will want to change it to
%%%% the specific modules for stats.

%%change_all(Mod, all, ToStatus) -> % typing in enable-all the riak-admin enables all stats on all nodes
%%  Stats = [exometer:info(Stat, name) || Stat <- ets:tab2list(exometer_util:table())],
%%  status_helper(Mod, Stats, ToStatus);
%%change_all(Mod, Stats, ToStatus) -> % enables these specific stats on all nodes
%%  status_helper(Mod, Stats, ToStatus).

%% enable_stat_for_me(all) -> % enables all stats on one node
%% enable_stat_for_me([Stats]) -> % enables specific stats on one node
%% enable_stat_for_them(all) -> % enable all stats on every node except this one
%% enable_stat_for_them([Stats]) -> % enables specific stats on all nodes except this one.

%% stats_diff(all) -> % show which stats are disabled or enabled which are different for each node
%%% return 'none' for when there is no difference between the stat difference.

%% force_update_all() -> % will check the values in exometer and change them if they are different to
% the data stored in the metadata
%% force_update_me() ->  % checks the values in exometer for this specific node and updates.

%%% ---- disable and enable specific stat types ---- %%%

%% disable_stat_mod(ModuleName, all) ->
%% disable_stat_mod(ModuleName, [Stats]) -> % [[~,~,node,gets],[~,~,node,gets,counter]]
%% disable_all_but_mod(ModuleName, all) ->
%% disable_all_but_mod(ModuleName, [Stats]) ->

%%% --------- Functions --------- %%%
%%
%%% call from riak-admin stat enable/disable ***
%%change_status(Mod, N, St) ->
%%  [{status, NewSt}] = check_meta(Mod, N, [{status, St}]),
%%  %% if the value St is the same as the one in the meta data,
%%  %% we still want to run it through the exometer, just because the
%%  %% value might be different in exometer, especially if the node
%%  %% restarts, value is automatically enabled.
%%
%%  %% we will always check and update the metadata first, mainly because
%%  %5 if a problem occurs mid-change the value is stored in meta
%%  case exometer:setopts(N, [{status, NewSt}]) of
%%    ok ->
%%      NewSt;
%%    Error ->
%%      Error
%%  end.
%%

%%
%%%% definitely needs reviewing
%%info_stat(Mod, N) ->
%%  E = exometer:info(N),
%%  case riak_core_metadata:get({Mod, ?STAT}, N) of % check metadata is = to
%%    Value when Value == E ->                      % exometer
%%      Value;
%%    Value when Value =/= E ->
%%      riak_core_metadata:put({Mod, ?STAT}, N, Value),
%%      Value
%%  end .
%%
%%show_stat(Pats) ->
%%  exometer:select(Pats).
%%
%%%%% --------- META Functions --------- %%%
%%
%%% check the metadata's value for the stat
%%check_meta(Mod, N, Opts) ->
%%  case riak_core_metadata:get({Mod, ?STAT}, N, Opts) of
%%    undefined ->
%%      %% first time changing opts
%%      riak_core_metadata:put({Mod, ?STAT}, N, Opts),
%%      Opts;
%%    Value when Opts == Value ->
%%      %% changing data to a value already saved in meta
%%      Value;
%%    Value when Opts =/= Value ->
%%      riak_core_metadata:put({Mod, ?STAT}, N, Opts),
%%      Opts   %% data has been changed, this is the value
%%  end.
%%

%%%%----------------Helper Functions----------------%%%%

%%change_helper(Mod, Stats, St) ->
%%  lists:foreach(fun(N) ->
%%    change_status(Mod, N, St)
%%                end, Stats).
