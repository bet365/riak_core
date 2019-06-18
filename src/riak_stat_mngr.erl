%%%-------------------------------------------------------------------
%%% @copyright (C) 2019, Bet365
%%% @doc
%%%
%%% All stats modules and riak_core_console call into this manager to
%%% communicate with exometer and the metadata.
%%%
%%% All the stats are saved in the metadata, when the node starts up
%%% it checks the metadata first for which stats to register into
%%% exometer, the status is kept in the values in exometer
%%%
%%% Data in the metadata is persisted, when a stat is enabled/disabled
%%% it will keep its status on restart.
%%%
%%% @end
%%% Created : 06. Jun 2019 14:34
%%%-------------------------------------------------------------------
-module(riak_stat_mngr).
-author("savannahallsop").

-behaviour(gen_server).

%%%%%%% TODO %%%%%%

% Todo: talk to Dine and Coxxy about persisting exometer data, is it something that
% we need to implement an easy method for someone to get to.
% i.e. storing data down to disk for a month then it is delete, or a week.
% e.g. if a node goes down and you want to see how long it takes for it to load back up
% or what the CPU or memory usage was or is you can riak-admin show "on-call" and it
% will show stats most useful for that profile.



%%% MANAGER

% Todo: organise the exports into the write exports sections

% Todo: consolidate functions that are similar

% Todo: move all generic functions into the assistant manager, data that
% goes into exometer needs to go into the metadata in a similar way, only
% exometer will be getting updated, however the stats need to be stored in
% a similar way.

% Todo: create profiles for individual teams for riak, these profiles can be stored in the metadata
% so when a certain profile is requested, it pulls down the list of stats names from metadata
% and then prints stats info for those stats
%
% possible profiles: stress testing puts, gets, deletes, on-call, bets, system, node, cluster.

% Todo: Specs
% Todo: make sure terms are correct for specs

% Todo: register_vnode_stats need to be in the register stats section, it is a separate function
% however it is called in from the riak_core_stat module and they can just be register automatically
% here.

%%% EXOM
% Todo: add vclocks to the stats that are added in when they are registered

% Todo: add vclocks to the stats when they are reset also

% Todo: write a function that deletes the stats data from exometer

%%% META

% Todo: write a function that remove the stat from the metadata, so when it is
% registered again it will be checked in metadata first.
% We could either include an option in metadata to say it is deleted, unless we store the
% stats in config or ets etc, then they are pulled from there and registered.
% then we dont have to remove hard coded stats

% Todo: write a function for incrementing the number of resets when the stat is
% re-registered or manually reset. this will keep track of when a node restarts

%%% CONSOLE

% Todo: write a function that will pull the enabled stats out of metadata,
% and check in exometer whether they have been updating during testing etc..
% and if they are not updating they will be returned

% Todo: write a function that will pull the list from the function above and
% will disable them for foreseeable.

% Todo: consolidate info and stat_info functions call from separate modules will a
% similar input and output.

% Todo: write a function in riak_core_console and riak-admin that will allow for
% specific profiles to be added/deleted/enabled/disbaled/showinfo etc...

% Todo: write a function that will reset the stats,
% it will change the number to 0 in exometer and update number of resets in meta by 1.


%%%% TEST %%%%

% Todo: write a function that checks the metadata status

% Todo: write a function that checks the exometer status



%%%%%% END TODO %%%%%



%%%%%% EXPORTED FUNCTIONS %%%%%%

-export([
  register_profiles/0, register_stats/4, set_method/1,
  add_profile/1, add_profile/2, add_profile_stat/2,
  remove_profile/1, remove_profile_stat/2]).

%% API riak_core_console
-export([
  show_stat/1, show_stat/2, stat0/1, disable0/1,
  get_stats/2, stat_info/1, info/2, stat_change/2,
  reset_stat/1, reset_stat/2, delete_stat/1, stat_opts/2
]).

% API riak_stat_meta_mgr
-export([
  register_meta_stats/4, check_meta/1, check_meta_info/1,
  delete_meta_stat/1, reset_meta_stat/1, set_meta_opts/2
]).

% API riak_stat_exom_mgr
-export([
  register_exom_stats/4, update_exom_stats/3, check_exom/1,
  check_exom_info/1, delete_exom_stat/1, reset_exom_stat/1,
  set_exom_opts/2
]).

% API riak_stat_cache_mgr
-export([]).

% API riak_stat_app_mgr
-export([]).

% need organising API
-export([
  change_status/3, register_stat/3,  register_vnode_stats/1,
  alias/1, aliases/2, get_values/1, get_value/1,
  unregister_stats/4, vnodeq_atom/2, show_pats/1,
  prefix/0, get_datapoints/2, get_datapoints/3, get_val_fol/4,
  get_fol_val/1, notify/3, start/0, stop/0,
  delete_metric/1, counter/1, history/2, gauge/1,
  register_stats/2, update_stats/3, new_register/3, update/4,
  get_stat/1
]).

-export([start_link/0]).
%% gen_server callbacks
-export([init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3]).

%%%%% MACROS %%%%%%
-define(SERVER, ?MODULE).
-define(STAT, stats).


%%%%% RECORDS %%%%%
-record(state, {
  method = lww,     % lww is the default
  profiles = orddict:new(), % profiles that include the {Profile, [Stats]}
  aggr = false}).     % default decision for stats, turn aggregation off.


%%%%% DOCUMENTATION %%%%%

%% #state{} =>
%% method -- lww | metadata | exometer
%%           choose the default method of reading/writing data.
%%           lww -- will read data from both exometer and metadata and
%%                  return the youngest value
%%           metadata -- returns the status of the stat from metadata
%%                  metadata does not store all the stat information
%%                  just the information needed to register in exometer
%%           exometer -- returns the information wanted specifically
%%                  from exometer, this is used automatically by:
%%                  "riak-admin stat info, riak.**..." as it holds all
%%                  the stats information.
%%
%% profiles -- orddict of the stats that are needed fot that specific
%%           profile, for example, a stress testing profile for puts
%%           to the vnode will only need a couple stats enabled, doing
%%           riak-admin stat enable-profile stress-puts will enable all
%%           the stats for that profile
%%          -- it can be used for reading stats as well, if all stats
%%           are enabled then the only stats you want returning will
%%           be stored will that profile.
%%
%% aggr -- true | false
%%         stats are aggregated when needed or called in the riak shell
%%         some are aggregated by default when returned in stat info,
%%         this allows extra functionality to see the specific stats in
%%         detail automatically.
%%
%% .

%%%===================================================================
%%% API
%%%===================================================================

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% GENERAL %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% register profiles in the orddict into metadata
-spec(register_profiles() -> ok).
register_profiles() ->
  gen_server:call(?SERVER, profiles).

% register_stats into exometer and metadata
-spec(register_stats(StatName :: list(), Type :: atom(), Opts :: list(), Aliases :: term()) ->
  term() | ok | {error, Reason}).
register_stats(StatName, Type, Opts, Aliases) ->
  gen_server:call(?SERVER, {register, StatName, Type, Opts, Aliases}).

% set the default method
-spec(set_method(Method :: (metadata | exometer | lww)) -> ok).
set_method(Method) ->
  gen_server:call(?SERVER, {method, Method}).

%% "riak-admin stat add-profile stress-puts riak.riak_kv.vnode.puts.**"

% add a profile and stats
-spec(add_profile(ProfileName :: term()) -> term() | ok).
add_profile(ProfileName) ->
  add_profile(ProfileName, []).
-spec(add_profile(ProfileName :: term(), Stats :: list()) -> term() | ok).
add_profile(ProfileName, Stats) ->
  gen_server:call(?SERVER, {add_profile, ProfileName, Stats}). % add to orddict
  % register in metadata

% add a stat to the list in the profile
-spec(add_profile_stat(ProfileName :: atom(), Stat :: list()) -> term() | ok).
add_profile_stat(ProfileName, Stat) ->
  gen_server:call(?SERVER, {add_profile_stat, ProfileName, Stat}).

% remove the profile
-spec(remove_profile(ProfileName :: atom) -> term() | ok).
remove_profile(ProfileName) ->
  gen_server:call(?SERVER, {remove_profile, ProfileName}).

% remove a stat in that specific profile
-spec(remove_profile_stat(ProfileName :: atom(), Stat :: list()) -> term() | ok).
remove_profile_stat(ProfileName, Stat) ->
  gen_server:call(?SERVER, {remove_profile_stat, ProfileName, Stat}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec(register_stats(App :: atom(), Stats :: term()) -> term() | ok).
register_stats(App, Stats) ->
  % handle call will do the check and regiter in meta and exom
  gen_server:call(?SERVER, {register, App, Stats}).

-spec(update_stats(App :: atom(), Name :: term(), Arg :: term()) -> term() | ok).
update_stats(App, Name, Arg) ->
  gen_server:call(?SERVER, {update, App, Name, Arg}).

-spec(new_register(App :: atom(), Name :: term(), Arg :: term()) -> term() | ok ).
new_register(App, Name, Arg) ->
  gen_server:call(?SERVER, {new_stat, App, Name, Arg}).

-spec(update(App :: atom(), Name :: term(), UpdateVal :: term(), Type :: atom()) -> term() | ok).
update(App, Name, UpdateVal, Type) ->
  gen_server:call(?SERVER, {update, App, Name, UpdateVal, Type}).

-spec(get_stat(App :: atom()) -> term()).
get_stat(App) ->
  gen_server:cast(?SERVER, {get, App}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% CONSOLE %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec(show_stat(Arg :: term()) -> term()).
%% Default for show_stat(Arg) is enabled
show_stat(Arg) ->
  show_stat(Arg, enabled).

-spec(show_stat(Arg :: term(), Status :: atom()) -> term()).
%% Show enabled or disabled stats
show_stat(Arg, Status) ->
  gen_server:cast(?SERVER, {show, Arg, Status}).

% Check which stats haven't been updated during testing etc...,
% return the list of enabled stats
-spec(stat0(Arg :: term()) -> term()).
stat0(Arg) ->
  gen_server:cast(?SERVER, {stat0, Arg}).

% use function above to return unused stats, and then set the status of
% those stats to disabled.
-spec(disable0(Arg :: term()) -> term()).
disable0(Arg) ->
  gen_server:cast(?SERVER, {disable0, Arg}).

-spec(get_stats(Name :: list(), DataPoint :: term()) -> term()).
get_stats(Name, DataPoint) ->
  gen_server:cast(?SERVER, {get, Name, DataPoint}).

-spec(stat_info(Arg :: term()) -> term()).
stat_info(Arg) ->
  {Attrs, RestArg} = pick_info_attrs(split_arg(Arg)),
  [print_info(E, Attrs) || E <- find_entries(RestArg, '_')].

-spec(info(Name :: term(), Info :: term()) -> term()).
info(Name, Info) ->
  gen_server:cast(?SERVER, {info, Name, Info}).

-spec(stat_change(Arg :: term(), ToStatus :: atom()) -> term()).
stat_change(Arg, ToStatus) ->
  gen_server:cast(?SERVER, {change_status, Arg, ToStatus}).

-spec(reset_stat(Arg :: term()) -> term()).
reset_stat(Arg) ->
  gen_server:cast(?SERVER, {reset, Arg}).

-spec(reset_stat(Mod :: term(), N :: term()) -> term()).
reset_stat(_Mod, N) ->
  riak_stat_exom_mgr:reset(N).

-spec(delete_stat(Arg :: term()) -> term()).
delete_stat(Arg) ->
  gen_server:cast(?SERVER, {delete, Arg}).

-spec(stat_opts(Name :: term(), Item :: atom()) -> term()).
stat_opts(Name, Item) ->
  gen_server:cast(?SERVER, {options, Name, Item}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% METADATA %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% register stats in the metadata
-spec(register_meta_stats(Statname :: term(), Type :: atom(), Opts :: list(), Aliases :: term()) ->
  term() | ok).
register_meta_stats(Statname, Type, Opts, Aliases) ->
  riak_stat_meta_mgr:register_stat(Statname, Type, Opts, Aliases).
% todo: make register_stat -> register, more generic

% register the profiles into meta data
-spec(register_meta_profiles(ProfileName :: atom(), StatList :: list()) ->
  term() | ok | {error, Reason}).
register_meta_profiles(ProfileName, StatList) ->
  riak_stat_meta_mgr:register(ProfileName, StatList).

% return the status of the stat, similar to " riak-admin stat show"
-spec(check_meta(StatName :: term()) -> term() | {error, Reason}).
check_meta(Statname) ->
  % gen_server:call(
  riak_stat_meta_mgr:check(Statname).

% return all the info on the stat in meta, like "riak-admin stat info"
-spec(check_meta_info(StatName :: term()) -> term() | {error, Reason}).
check_meta_info(Statname) ->
  % gen_server:call(
  riak_stat_meta_mgr:info(Statname).

% deletes the stat entry from the metadata
-spec(delete_meta_stat(StatName :: term()) -> term() | ok | {error, Reason}).
delete_meta_stat(Statname) ->
  % gen_server:call(
  riak_stat_meta_mgr:delete(Statname).

% resets the options and additional values back to the default
-spec(reset_meta_stat(StatName :: term()) -> term() | ok | {error, Reason}).
reset_meta_stat(Statname) ->
  % gen-server:call(
  riak_stat_meta_mgr:reset_stat(Statname ,[]).

% set the options for the stat or stats, default options are added on registration
-spec(set_meta_opts(StatName :: term(), Opts :: list()) -> term() | ok | {error, Reason}).
set_meta_opts(Statname, Opts) ->
  % gen_server:call(
  riak_stat_meta_mgr:set_opts(Statname, Opts). % lists:keyreplace

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% EXOMETER %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% register the stats in exometer
-spec(register_exom_stats(StatName :: list(), Type :: atom(), Opts :: list(), Aliases :: list()) ->
  term() | ok | {error, Reason}).
register_exom_stats(Statname, Type, Opts, Aliases) ->
  riak_stat_exom_mgr:register_stat(Statname, Type, Opts, Aliases).

% update the stats in exometer
-spec(update_exom_stats(App :: atom(), Name :: term(), Arg :: term()) -> term() | ok | {error, Reason}).
update_exom_stats(App, Name, Arg) ->
  riak_stat_exom_mgr:update(App, Name, Arg).

% similar to "riak-admin stat show"
-spec(check_exom(Statname :: list()) -> term() | ok | {error, Reason}).
check_exom(Statname) ->
  riak_stat_exom_mgr:check(Statname).

% similar to "riak-admin stat info"
-spec(check_exom_info(StatName :: term()) -> term() | ok | {error, Reason}).
check_exom_info(Statname) ->
  riak_stat_exom_mgr:info(Statname).

% delete the stat from exometer, also updates the stat in metadata as deleted
-spec(delete_exom_stat(StatName :: term()) -> term() | ok | {error, Reason}).
delete_exom_stat(Statname) ->
  riak_stat_exom_mgr:delete(Statname).

% reset the stat in exometer back to the defaults
-spec(reset_exom_stat(StatName :: term()) -> term() | ok | {error, Reason}).
reset_exom_stat(Statname) ->
  riak_stat_exom_mgr:reset(Statname).

% change the opts in exometer manually etc
-spec(set_exom_opts(StatName :: term(), Opts :: list()) -> term() | ok | {error, Reason}).
set_exom_opts(Statname, Opts) ->
  riak_stat_exom_mgr:set_opts(Statname, Opts). % this will be called during registration
  % this checks the method, if the default is metadata then it will set the options from there
  % if it is exometer it will have exometers default opts
  % if it is lww then it is comparing the youngest.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% CACHING %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% APP %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%% METADATA %%%%

%

%%%% TEST %%%%


start() ->
  riak_stat_exom_mgr:start().

stop() ->
  riak_stat_exom_mgr:stop().

register_vnode_stats(Stats) ->
  gen_server:cast(?SERVER, {vnode_stats, Stats}).

alias(Arg) ->
  gen_server:cast(?SERVER, {alias, Arg}).

aliases(Type, Args) ->
  gen_server:cast(?SERVER, {aliases, Type, Args}).

get_value(Stat) ->
  riak_stat_exom_mgr:get_value(Stat).

get_values(Path) ->
  gen_server:cast(?SERVER, {path, Path}).

unregister_stats(Module, Index, Type, App) ->
  gen_server:cast(?SERVER, {unregister, Module, Index, Type, App}).

vnodeq_atom(Service, Desc) ->
  binary_to_atom(<<(atom_to_binary(Service, latin1))/binary, Desc/binary>>, latin1).

show_pats(Pats) ->
  riak_stat_exom_mgr:select(Pats).

get_datapoints(Name, Type) ->
  riak_stat_exom_mgr:get_datapoints(Name, Type).
get_datapoints(Name, Type, Opts) ->
  riak_stat_exom_mgr:get_datapoints(Name, Type, Opts).

get_val_fol(Name, Type, Opts, DPs) ->
  riak_stat_exom_mgr:get_value_folsom(Name, Type, Opts, DPs).

get_fol_val(Arg) ->
  riak_stat_exom_mgr:get_metric_value(Arg).

notify(Name, Val, Type) ->
  riak_stat_exom_mgr:notify_metric(Name, Val, Type).

delete_metric(Arg) ->
  riak_stat_exom_mgr:delete_metric(Arg).

counter(Arg) ->
  riak_stat_exom_mgr:new_counter(Arg).

history(Name, Arg) ->
  riak_stat_exom_mgr:new_history(Name, Arg).

gauge(Arg) ->
  riak_stat_exom_mgr:new_gauge(Arg).

%%%% CACHE %%%%

-spec(start_link() ->
  {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

-spec(init(Args :: term()) ->
  {ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term()} | ignore).
init([]) ->

  % tODO: register profiles in this init
  % register stats as well?

  {ok, #state{}}.


%%%%% TODO: HANDLE CALL %%%%%

% Todo: make register_stat register in exometer and in metadata

% Todo: update only updates the stat in exometer

% Todo: can new_stat be consolidated into register stat

% Todo: consolidate the update stats into the same function, as the only update
% is going to be in exometer. todo: change to update or create

-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: #state{}) ->
  {reply, Reply :: term(), NewState :: #state{}} |
  {reply, Reply :: term(), NewState :: #state{}, timeout() | hibernate} |
  {noreply, NewState :: #state{}} |
  {noreply, NewState :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term(), Reply :: term(), NewState :: #state{}} |
  {stop, Reason :: term(), NewState :: #state{}}).
handle_call(profiles, _From, State = #state{profiles = Profiles}) ->
  orddict:map(fun(ProfileName, StatList) ->
    register_meta_profiles(ProfileName, StatList)
              end, Profiles),
  {reply, ok, State};

handle_call({register, App, Stats}, _From, State) ->
  lists:foreach(fun(Stat) ->
    register_stat(prefix(), App, Stat)
                end, Stats),
  {reply, ok, State};
handle_call({update, App, Name, Arg}, _From, State) ->
  StatName = stat_name(prefix(), App, Name),
  Reply =
    case riak_stat_exom_mgr:update(StatName, Arg) of
      {error, not_found} ->
        lager:debug("~p not found on update.~n", [Name]);
      ok ->
        ok
    end,
  {reply, Reply, State};
handle_call({new_stat, App, Name, Arg}, _From, State) ->
  StatName = stat_name(prefix(), App, Name),
  riak_stat_exom_mgr:register_stat(StatName, Arg),
  {reply, ok, State};
handle_call({update, App, Name, UpdateVal, Type}, _From, State) ->
  StatName = stat_name(prefix(), App, Name),
  riak_stat_exom_mgr:update_or_create(StatName, UpdateVal, Type),
  {reply, ok, State};

handle_call(_Request, _From, State) ->
  {reply, ok, State}.

%%%%% TODO: Handle_cast %%%%%

% Todo: write the function for stat0, finds enabled stats from method,
% then find which enabled stats havent been updated, or returns no stats

% Todo: write the function for disable0, same as above but calls into
% the disable_stat and disables it in meta and exometer.





-spec(handle_cast(Request :: term(), State :: #state{}) ->
  {noreply, NewState :: #state{}} |
  {noreply, NewState :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #state{}}).

handle_cast({show, Arg, Status}, State = #state{method = Priority}) ->
  print_stats(find_entries(Priority, Arg, Status)),
  {noreply, State};

handle_cast({info, Name, Item}, State) ->
  %%info_stat(Mod, N) ->
%%  E = exometer:info(N),
%%  case riak_core_metadata:get({Mod, ?STAT}, N) of % check metadata is = to
%%    Value when Value == E ->                      % exometer
%%      Value;
%%    Value when Value =/= E ->
%%      riak_core_metadata:put({Mod, ?STAT}, N, Value),
%%      Value
%%  end .
  case riak_stat_meta_mgr:info_stat(Name, Item) of
    undefined ->
      {ok, Value} = riak_stat_exom_mgr:info(Name, Item),
      Value;
    {ok, Value} ->
      Value
  end,
  %% TODO: check the output of the exom and metadata
  {noreply, State};
% TODO: add register stats
handle_cast({vnode_stats, Stats}, State) ->
  % TODO: add the vnode stats into metadata as well
  lists:foreach(fun
                  ({ensure, Stat, Type, Alias}) ->
                    riak_stat_exom_mgr:ensure(Stat, Type, Alias);
                  ({re_register, Stat, Type}) ->
                    riak_stat_exom_mgr:re_register(Stat, Type)
                end, Stats),
  {noreply, State};
handle_cast({unregister, [Op, time], Index, Type, App}, State) ->
  riak_stat_exom_mgr:delete([prefix(), App, Type, Op, time, Index]),
  % TODO: unregister from metadata
  {noreply, State};
handle_cast({unregister, Module, Index, Type, App}, State) ->
  riak_stat_exom_mgr:delete([prefix(), App, Type, Module, Index]),
  % TODO: unregister from metadata (call helper)
  {noreply, State};
handle_cast({change_status, Arg, ToStatus}, State = #state{method = Pr}) ->
  lists:foreach(
    fun({[{LP, []}], _}) ->
      io:fwrite(
        "== ~s (Legacy pattern): No matching stats ==~n", [LP]);
      ({[{LP, Matches}], _}) ->
        io:fwrite("== ~s (Legacy pattern): ==~n", [LP]),
        [io:fwrite("~p: ~p~n", [N, change_status(N, ToStatus)])
          || {N, _} <- Matches];
      ({[], _}) ->
        io:fwrite("No matching stats~n", []);
      ({Entries, _}) ->
        [io:fwrite("~p: ~p~n", [N, change_status(N, ToStatus)])
          || {N, _, _} <- Entries]
    end, find_entries(Pr, Arg, '_')),
% TODO: check the metadata for the status of the stat,

% if it is be enabled and is already enabled it will ignore it

  {noreply, State};
%% if the value St is the same as the one in the meta data,
%% we still want to run it through the exometer, just because the
%% value might be different in exometer, especially if the node
%% restarts, value is automatically enabled.

%% we will always check and update the metadata first, mainly because
%5 if a problem occurs mid-change the value is stored in meta,
% TODO: call into exom and reset stat as well.
handle_cast({reset, Arg}, State) ->
  lists:foreach(
    fun({[{LP, []}], _}) ->
      io:fwrite(
        "== ~s (Legacy pattern): No matching stats ==~n", [LP]);
      ({[{LP, Matches}], _}) ->
        io:fwrite("== ~s (Legacy pattern): ==~n", [LP]),
        [io:fwrite("~p: ~p~n", [N, riak_stat_meta_mgr:reset_stat(core, N)])
          || {N, _} <- Matches];
      ({Entries, _}) ->
        [io:fwrite("~p: ~p~n", [N, riak_stat_meta_mgr:reset_stat(core, N)])
          || {N, _, _} <- Entries]
    end, find_entries(Arg, enabled)),
  {noreply, State};
handle_cast({options, Name, Item}, State = #state{method = Method}) ->
  NewState =
    case Name of
      method when Item =/= Method, Item == lww; Item == priority ->
        State#state{method = Item};
      method when Item == Method ->
        lager:warning("method already chosen~n");
      priority when Item =/= Method ->
        NewP =
          case Item of
            metadata -> meta;
            meta -> meta;
            exometer -> exom;
            exom -> exom;
            _ ->
              lager:warning("invalid entry~n")
          end,
        State#state{method = NewP};
      _ ->
        lager:warning("invalid data entry~n")
    end,
  {noreply, NewState};
handle_cast({get, App}, State) ->
  riak_stat_exom_mgr:get_values([prefix(), App]),
  {noreply, State};
handle_cast({get, Name, DataPoint}, State) ->
  riak_stat_exom_mgr:get_value(Name, DataPoint),
  {noreply, State};
handle_cast({alias, Arg}, State) ->
  riak_stat_exom_mgr:alias(Arg),
  {noreply, State};
handle_cast({aliases, Type, Arg}, State) ->
  riak_stat_exom_mgr:aliases(Type, Arg),
  {noreply, State};
handle_cast({path, Path}, State) ->
  riak_stat_exom_mgr:get_values(Path),
  {noreply, State};


handle_cast(_Request, State) ->
  {noreply, State}.


-spec(handle_info(Info :: timeout() | term(), State :: #state{}) ->
  {noreply, NewState :: #state{}} |
  {noreply, NewState :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #state{}}).
handle_info(_Info, State) ->
  {noreply, State}.

-spec(terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: #state{}) -> term()).
terminate(_Reason, _State) ->
  ok.

-spec(code_change(OldVsn :: term() | {down, term()}, State :: #state{},
    Extra :: term()) ->
  {ok, NewState :: #state{}} | {error, Reason :: term()}).
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

prefix() ->
  app_helper:get_env(riak_core, stat_prefix, riak).

%%%===================================================================
%%% Register Stats
%%%===================================================================

register_stat(P, App, Stat) ->
  {Name, Type, Opts, Aliases} =
    case Stat of
      {N, T} -> {N, T, [], []};
      {N, T, Os} -> {N, T, Os, []};
      {N, T, Os, As} -> {N, T, Os, As}
    end,
  StatName = stat_name(P, App, Name),
  % TODO: pull out the metadata Opts to pass into exom
  NewOpts = register_meta(StatName, Type, Opts, Aliases),
  register_exom(StatName, Type, NewOpts, Aliases).

% TODO: Have update_stats call into this as well

stat_name(P, App, N) when is_atom(N) ->
  stat_name_([P, App, N]);
stat_name(P, App, N) when is_list(N) ->
  stat_name_([P, App | N]).

stat_name_([P, [] | Rest]) -> [P | Rest];
stat_name_(N) -> N.

register_meta(StatName, Type, Opts, Aliases) ->
  riak_stat_meta_mgr:register_stat(StatName, Type, Opts, Aliases).

register_exom(StatName, Type, Opts, Aliases) ->
  riak_stat_exom_mgr:re_register(StatName, Type, Opts),
  lists:foreach(
    fun({DP, Alias}) ->
      riak_stat_exom_mgr:alias(new, [Alias, StatName, DP])
    end, Aliases).

check_meta(_Mod, _Name, _Opts) ->
  % TODO: write a function that checks the opts of the stat
%% we will always check and update the metadata first, mainly because
%5 if a problem occurs mid-change the value is stored in meta


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
  ok.

%%%===================================================================
%%% Stat show/info
%%%===================================================================

print_stat(Arg) -> % Todo: make the functions for stat info and stat show call this
  print_stat(Arg, []).


print_stat([], []) ->
  io:fwrite("No Matching stats~n", []);

print_stat({[], _}, _) ->
  io_lib:fwrite("No matching stats~n", []);

print_stat({[{LP, []}], _}, _) ->
  io:fwrite("== ~s (Legacy pattern): No matching stats ==~n", [LP]);

print_stat({[{LP, Matches}], _}, Attrs) ->
  io:fwrite("== ~s (Legacy pattern): ==~n", [LP]),
  lists:foreach(
    fun({N, _}) ->
      print_info_1(N, Attrs)
    end, Matches);

print_stat({Entries, _}, Attrs) ->
  lists:foreach(
    fun({N, _, _}) ->
      print_info_1(N, Attrs)
    end, Entries);

print_stat(Entries, []) ->
  lists:foreach(
    fun({[{LP, []}], _}) ->
      io:fwrite(
        "== ~s (Legacy pattern): No matching stats ==~n", [LP]);
      ({[{LP, Matches}], _}) ->
        io:fwrite("== ~s (Legacy pattern): ==~n", [LP]),
        [[io:fwrite("~p: ~p (~p/~p)~n", [N, V, E, DP])
          || {DP, V, N} <- DPs] || {E, DPs} <- Matches];
      ({[], _}) ->
        io:fwrite("No matching stats~n", []);
      ({Entries1, DPs}) ->
        [io:fwrite("~p: ~p~n", [E, get_value(E, Status, DPs)])
          || {E, _, Status} <- Entries1]
    end, Entries).


% TODO: make print_stats and print info similar
print_stats([]) ->
  io:fwrite("No matching stats~n", []);
print_stats(Entries) ->
  lists:foreach(
    fun({[{LP, []}], _}) ->
      io:fwrite(
        "== ~s (Legacy pattern): No matching stats ==~n", [LP]);
      ({[{LP, Matches}], _}) ->
        io:fwrite("== ~s (Legacy pattern): ==~n", [LP]),
        [[io:fwrite("~p: ~p (~p/~p)~n", [N, V, E, DP])
          || {DP, V, N} <- DPs] || {E, DPs} <- Matches];
      ({[], _}) ->
        io:fwrite("No matching stats~n", []);
      ({Entries1, DPs}) ->
        [io:fwrite("~p: ~p~n", [E, get_value(E, Status, DPs)])
          || {E, _, Status} <- Entries1]
    end, Entries).
% TODO: Move into the assistant manager

pick_info_attrs(Arg) ->
  case lists:foldr(
    fun("-name", {As, Ps}) -> {[name | As], Ps};
      ("-type", {As, Ps}) -> {[type | As], Ps};
      ("-module", {As, Ps}) -> {[module | As], Ps};
      ("-value", {As, Ps}) -> {[value | As], Ps};
      ("-cache", {As, Ps}) -> {[cache | As], Ps};
      ("-status", {As, Ps}) -> {[status | As], Ps};
      ("-timestamp", {As, Ps}) -> {[timestamp | As], Ps};
      ("-options", {As, Ps}) -> {[options | As], Ps};
      (P, {As, Ps}) -> {As, [P | Ps]}
    end, {[], []}, Arg) of
    {[], Rest} ->
      {[name, type, module, value, cache, status, timestamp, options], Rest};
    Other ->
      Other
  end.

% TOdo: this could be changed to be similar to the print_stats, function can be taken out
print_info({[{LP, []}], _}, _) ->
  io:fwrite("== ~s (Legacy pattern): No matching stats ==~n", [LP]);
print_info({[{LP, Matches}], _}, Attrs) ->
  io:fwrite("== ~s (Legacy pattern): ==~n", [LP]),
  lists:foreach(
    fun({N, _}) ->
      print_info_1(N, Attrs)
    end, Matches);
print_info({[], _}, _) ->
  io_lib:fwrite("No matching stats~n", []);
print_info({Entries, _}, Attrs) ->
  lists:foreach(
    fun({N, _, _}) ->
      print_info_1(N, Attrs)
    end, Entries).


print_info_1(N, [A | Attrs]) ->
  Hdr = lists:flatten(io_lib:fwrite("~p: ", [N])),
  Pad = lists:duplicate(length(Hdr), $\s),
  Info = riak_stat_meta_mgr:info_stat(core, N), % Todo: change to info stat call in this mngr
  Status = proplists:get_value(status, Info, enabled),
  Body = [io_lib:fwrite("~w = ~p~n", [A, proplists:get_value(A, Info)])
    | lists:map(fun(value) ->
      io_lib:fwrite(Pad ++ "~w = ~p~n",
        [value, get_value(N, Status, default)]);
      (Ax) ->
        io_lib:fwrite(Pad ++ "~w = ~p~n",
          [Ax, proplists:get_value(Ax, Info)])
                end, Attrs)],
  io:put_chars([Hdr, Body]).

split_arg([Str]) ->
  re:split(Str, "\\s", [{return, list}]).


find_entries(exom, Arg) ->
  riak_stat_exom_mgr:find_entries(Arg);
find_entries(meta, Arg) ->
  riak_stat_meta_mgr:find_entries(Arg).
find_entries(exom, Arg, enabled) ->
  riak_stat_exom_mgr:find_entries(Arg);
find_entries(meta, Arg, enabled) ->
  riak_stat_meta_mgr:find_entries(Arg);
find_entries(exom, Arg, disabled) ->
  riak_stat_exom_mgr:find_entries(Arg, disabled);
find_entries(meta, Arg, disabled) ->
  riak_stat_meta_mgr:find_entries(Arg, disabled).

get_value(_, disabled, _) ->
  disabled;
get_value(E, _Status, DPs) ->
  case get_stats(E, DPs) of
    {ok, V} -> V;
    {error, _} -> unavailable
  end.


%%%===================================================================
%%% change status
%%%===================================================================

change_status(N, St) ->
  riak_stat_meta_mgr:change_status(core, N, St).

change_status(Mod, Name, ToStatus) ->
  % TODO: check the metadata for the status of the stat,
  % if it is be enabled and is already enabled it will ignore it
  case check_meta(Mod, Name, [{status, ToStatus}]) of
    [{status, ToStatus}] ->
      ToStatus;
    [{status, NewStatus}] ->
      % TODO: call riak_stat_exom_mgr
      case exometer:setopts(Name, [{status, NewStatus}]) of
        ok ->
          NewStatus;
        Error ->
          Error
      end
  end.
%% if the value St is the same as the one in the meta data,
%% we still want to run it through the exometer, just because the
%% value might be different in exometer, especially if the node
%% restarts, value is automatically enabled.


%% TODO: helper function for unregistering stats

%% TODO: helper for meta key
%% TODO: helper for meta bucket
%% TODO: helper for meta opts


%%%===================================================================
%%% Update stats
%%%===================================================================


%%%===================================================================
%%% Delete/reset Stats
%%%===================================================================