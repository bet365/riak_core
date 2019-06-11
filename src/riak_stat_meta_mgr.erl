%%%-------------------------------------------------------------------
%%% @copyright (C) 2019, Bet365
%%% @doc
%%% Middle-man for the stats/consoles and exometer/metadata
%%% @end
%%% Created : 04. Jun 2019 11:39
%%%-------------------------------------------------------------------
-module(riak_stat_meta_mgr).

%% API
-export([find_entries/1, find_entries/2]).

-export([register_stat/4]).

-define(PFX, fun riak_stat_mngr:prefix/0).
-define(STAT, stats).


%%%-------------------------------------------------------------------
%%% API
%%%-------------------------------------------------------------------

find_entries(Arg) ->
  find_entries(Arg, enabled).

find_entries(Arg, Status) ->
  riak_stat_assist_mgr:find_meta_entries(Arg, Status).

% TODO: actually write functions for the metadata
% TODO: abstract it all out
% TODO: below




register_stat(StatName, Type, Opts, Aliases) ->
  case check_meta(StatName) of
    undefined ->
      re_register_stat(StatName, Type, Opts, Aliases),
      Opts;
    {ok, {StatName, Type, NewOpts}} ->
      NewOpts
  %% TODO: when Opts == NewOpts
  end.

check_meta(StatName) ->
  %% TODO: pull out status
  opts(StatName),
  riak_core_metadata:get({metadata, ?STAT}, StatName).

re_register_stat(StatName, Type, Opts, Aliases) ->
  riak_core_metadata:put({metadata, ?STAT}, StatName, {Type, [{resets,0} |Opts], Aliases}).

opts(StatName) ->
  % check meta for Opts,
  % pull out the status
  % return that for exometer.
  StatName.




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
%%reset_stat(_Mod, N) ->
%%  exometer:reset(N)
%%%% get the data from exometer again and put it back into the meta data
%%%% or do its own metadata reset. similar to the exometer reset
%%.
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
