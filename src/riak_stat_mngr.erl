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

% Todo: add vclocks to the stats that are added in when they are registered

% Todo: add vclocks to the stats when they are reset also

%%%%%% EXPORTED FUNCTIONS %%%%%%

%% General API
-export([
  register_stats/2, register_vnode_stats/2,
  get_stats/1, get_info/2, get_datapoint/2, get_value/1, get_values/1,
  update_or_create/4, unregister_stats/4, reset_stats/1]).

%% Profile API
-export([
  load_profile/1, reset_profiles/0,
  add_profile/1, add_profile_stat/1,
  remove_profile/1, remove_profile_stat/1,
  change_profile_stat/1, check_profile_stat/1]).

%% API riak_core_console
-export([
  show_stat/2, stat0/1, disable0/1,
  stat_info/1, stat_change/2, reset_stat/1]).

% API riak_stat_meta_mgr
-export([
  register_meta_stats/4, register_meta_profiles/2,
  check_meta_status/2, check_meta_info/1, change_meta_status/2,
  unreg_meta_stat/1, reset_meta_stat/1, set_meta_opts/2]).

% API riak_stat_exom_mgr
-export([
  register_exom_stats/4, alias/1, aliases/2,
  update_exom_stats/3, update_exom_stats/4, change_exom_status/2,
  check_exom_info/2, select_stat/1,
  unreg_exom_stat/1, reset_exom_stat/1, set_exom_opts/2]).

% Additional API
-export([
  start/0, stop/0, get_datapoints/2, get_datapoints/3, get_val_fol/4,
  notify/3, delete_metric/1, vnodeq_atom/2, prefix/0, the_alpha_stat/2,
  meta_keyer/2, sanitise/1, change_these_stats/1, timestamp/0, stat_name/3]).

-export([counter/1, history/2, gauge/1]).

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
-define(TIMEOUT, 60000).
-define(time, vclock:fresh()). %% Todo: decide if we want a timestamp or vclock
-define(timestamp, timestamp()).

%%%%% RECORDS %%%%%
-record(state, {
  method = lww,     % lww is the default
  profile}). % profiles that include the {Profile, [Stats]}
%%  aggr = false}).     % default decision for stats, turn aggregation off. % TODO: Do we need this aggr?

%%%%% @doc %%%%%

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
%% profiles -- ets of the stats that are needed fot that specific
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

%% @end

%%%===================================================================
%%% API
%%%===================================================================

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% GENERAL %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec(register_stats(App :: atom(), Stats :: term()) ->
  {error, Reason :: term()} | ok).
%% @doc
%% All stats that get registered go through metadata first, the status
%% of the stats already registered is checked and returned to exometer
%%
%% Exometer will register the stats and enable them automatically,
%% pulling the status from the metadata and placing it into the options
%% given to exometer can set the status when it is registered.
%% preventing the stats having to be manually enabled/disabled on
%% start up.
%%
%% Exometer registration returns -> ok unless there is an {error, Reason}
%% @end
register_stats(App, Stats) ->
  gen_server:call(?MODULE, {register, App, Stats}).

%% Now unused, as the data for vnode stats has been altered to fit register stats
-spec(register_vnode_stats(App :: atom(), Stats :: list()) -> ok | term()).
%% @doc
%% vnode stats are from riak_core_stat and require a different type of
%% registration
%% @end
register_vnode_stats(App, Stats) ->
  gen_server:call(?SERVER, {vnode_stats, App, Stats}).

-spec(get_stats(App :: atom()) -> term()).
%% @doc
%% In riak attach the function get_stats() can be called to retrieve all
%% the stats that is registered to that particular app.
%%
%% get_stats/1 can be used to retrieve stats in a particular path not just app.
%% @end
get_stats(App) ->
  Values = gen_server:call(?SERVER, {get_app_stats, App}),
  [print_stats(Name, [status]) || {Name, _V} <- Values].

-spec(get_info(Name :: term(), Info :: term()) -> term()).
%% @doc
%% Get info on the specific data you want from exometer
%% @end
get_info(Name, Info) ->
  gen_server:call(?SERVER, {info, Name, Info}).

-spec(get_datapoint(Name :: list() | term(), DataPoint :: term()) -> term()).
%% @doc
%% Retrieve the current value of a stat from exometer.
%% @end
get_datapoint(Name, DataPoint) ->
  gen_server:call(?SERVER, {get_datapoint, Name, DataPoint}).

-spec(get_value(Stat :: list()) -> ok | term()).
%% @doc
%% Same as the function above, except in exometer the Datapoint:
%% 'default' is inputted, however it is used by some modules
%% @end
get_value(Stat) ->
  riak_stat_exom_mgr:get_value(Stat).

-spec(get_values(Path :: list() | term()) -> term()).
%% @doc
%% get the values from exometer using the Path provided.
%% @end
get_values(Path) ->
  riak_stat_exom_mgr:get_values(Path).

-spec(update_or_create(App :: atom(), Name :: term(), UpdateVal :: term(), Type :: atom()) ->
  term() | ok).
%% @doc
%% Updates the Stats by a specific value given, unless the value does not
%% exists then it creates the stat in exometer. It calls the update/2 in
%% exometer, hence why the update_stats function was removed.
%% @end
update_or_create(App, Name, UpdateVal, Type) ->
  gen_server:call(?SERVER, {update, App, Name, UpdateVal, Type}).

-spec(unregister_stats(Module :: atom() | list(), Index :: term(),
    Type :: atom() | term(), App :: atom() | term()) -> ok | term()).
%% @doc
%% Un-registering the stats deletes them from exometer and leaves the
%% stat in exometer with the Option: {unregistered, true},
%% upon re_registering again the the stat will be ignored unless the
%% option for unregistered is set manually to 'false' in the metadata.
%% @end
unregister_stats(Module, Index, Type, App) ->
  gen_server:call(?SERVER, {unregister, Module, Index, Type, App}).


reset_stats(Name, []) ->
  reset_stats(Name).
-spec(reset_stats(Name :: term()) -> term()).
%% @doc
%% Reset the stats in exometer and metadata and keeps record of how
%% many times it has been reset in the metadata
%% @end
reset_stats(Name) ->
  case reset_meta_stat(Name) of
    ok ->
      reset_exom_stat(Name);
    _ ->
      io:fwrite("Could not reset stat ~p~n", [Name])
  end.

%%% DEFAULTS %%%

%%-spec(set_method(Method :: (metadata | exometer | lww)) -> ok).
%%%% @doc
%%%% Set the default method in which the data should be written.
%%%% default is lww, which checks/writes to both exometer and the
%%%% metadata, returning the option with the youngest time.
%%%% @end
%%set_method(Method) ->
%%  gen_server:call(?SERVER, {method, Method}).


%%%%%%%%%% PROFILES %%%%%%%%%%%

-spec(load_profile(ProfileName :: term()) -> term() | ok | {error, Reason :: term()}).
%% @doc
%% loads a profile from the metadata, if it does not exist it will
%% return an error
%% @end
load_profile(ProfileName) ->
  gen_server:call(?SERVER, {load, ProfileName}).

-spec(reset_profiles() -> term() | {error, Reason :: term()}).
%% @doc
%% resets all the stats back to enabled and removes the current loaded
%% profile
%% If no profile is loaded it will return an error
%% @end
reset_profiles() ->
  gen_server:call(?SERVER, reset_profile).

-spec(add_profile(ProfileName :: term()) -> term() | ok | {error, Reason :: term()}).
%% @doc
%% Add the profile and the stats
%% @end
add_profile(ProfileName) ->
  gen_server:call(?SERVER, {add_profile, ProfileName}). % add to orddict

-spec(add_profile_stat(Stat :: list()) -> term() | ok | {error, Reason :: term()}).
%% @doc
%% add a stat to the list in the profile
%% @end
add_profile_stat(Stat) ->
  gen_server:call(?SERVER, {add_profile_stat, Stat}).

-spec(remove_profile(ProfileName :: atom) -> term() | ok | {error, Reason :: term()}).
%% @doc
%% remove the profile from the metadata
%% @end
remove_profile(ProfileName) ->
  gen_server:call(?SERVER, {remove_profile, ProfileName}).

-spec(remove_profile_stat(Stat :: list()) -> term() | ok | {error, Reason :: term()}).
%% @doc
%% remove a stat in that specific profile
%% @end
remove_profile_stat(Stat) ->
  gen_server:call(?SERVER, {remove_profile_stat, Stat}).

-spec(change_profile_stat(Stat :: list()) -> term() | ok | {error, Reason :: term()}).
%% change a status of a stat in the profile loaded, changes in permanently in the
%% profile and changes it in the metadata
%% @end
change_profile_stat(Stat) ->
  gen_server:call(?SERVER, {change_profile_stat, Stat}).

-spec(check_profile_stat(Stat :: list()) -> term() | ok | {error, Reason :: term()}).
%% @doc
%% check a status of a stat in the current profile
%% checks the file and the metadata, if there is a difference the file
%% takes authority as default
%% @end
check_profile_stat(Stat) ->
  gen_server:call(?SERVER, {check_profile_stat, Stat}).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% CONSOLE %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec(show_stat(Arg :: term(), Status :: atom()) -> term()).
%% @doc
%% Show enabled or disabled stats
%% when using riak-admin stat show riak.** enabled stats will show by default
%%
%% otherwise use: riak-admin stat show-enabled | show-disabled
%% @end
show_stat(Arg, Status) ->
  Reply = gen_server:call(?SERVER, {show, Arg, Status}),
  print_stats(Reply, []).

-spec(stat0(Arg :: term()) -> term()).
%% @doc
%% Check which stats in exometer are not updating, only checks enabled
%% @end
stat0(Arg) ->
  NotUpdating = gen_server:call(?SERVER, {stat0, Arg}),
  print(NotUpdating).

-spec(disable0(Arg :: term()) -> term()).
%% @doc
%% Similar to the function above, but will disable all the stats that
%% are not updating
%% @end
disable0(Arg) ->
  gen_server:call(?SERVER, {disable0, Arg}).

-spec(stat_info(Arg :: term()) -> term()).
%% @doc
%% Returns all the stats informtation
%% @end
stat_info(Arg) ->
  {Attrs, RestArg} = gen_server:call(?SERVER, {info_stat, Arg}),
  [print_stats(E, Attrs) || E <- find_entries(RestArg, '_')].

-spec(stat_change(Arg :: term(), ToStatus :: atom()) -> term()).
%% @doc
%% change the status of the stat in metadata and in exometer
%% @end
stat_change(Arg, ToStatus) ->
  OldStatus = gen_server:call(?SERVER, {change_status, Arg, ToStatus}),
  responder(Arg, OldStatus, fun change_status/2, ToStatus).

-spec(reset_stat(Arg :: term()) -> term()).
%% @doc
%% resets the stats in metadata and exometer and tells metadata that the stat
%% has been reset
%% @end
reset_stat(Arg) ->
%%  gen_server:call(?SERVER, {reset, Arg}).
  responder(Arg, enabled, fun reset_stats/2, []).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% METADATA %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec(register_meta_stats(Statname :: term(), Type :: atom(), Opts :: list(), Aliases :: term()) ->
  term() | ok | {error, Reason :: term()}).
%% @doc
%% registers the stats in metadata if they aren't already there, If they are
%% there it returns the status options for exometer to put.
%%
%% If a stat has the option {unregistered, true} it will
%% be ignored going into exometer.
%% @end
register_meta_stats(Statname, Type, Opts, Aliases) ->
  riak_stat_meta_mgr:register_stat(Statname, Type, Opts, Aliases).

-spec(register_meta_profiles(ProfileName :: atom() | term(), StatList :: list()) ->
  term() | ok | {error, Reason :: term()}).
%% @doc
%% register the profile name and the stats into the metadata
%% @end
register_meta_profiles(ProfileName, StatList) ->
  riak_stat_meta_mgr:add_profile(ProfileName, StatList).

-spec(check_meta_status(Arg :: term(), Status :: atom()) -> term() | {error, Reason :: term()}).
%% @doc
%% Checks the status that is in the metadata
%% @end
check_meta_status(Arg, Status) ->
  [riak_stat_meta_mgr:check_status(Name) || {{Name, _, _S}, _DPs} <- find_entries(Arg, Status)].

-spec(check_meta_info(StatName :: term()) -> term() | {error, Reason :: term()}).
%% @doc
%% Checks the information for that stat in the metadata
%% @end
check_meta_info(Arg) ->
%%  [
    riak_stat_meta_mgr:check_meta(Arg)
%%    || {{Name, _, _S}, _DPs} <- find_entries(Arg, '_')]
.

-spec(change_meta_status(Name :: term(), ToStatus :: atom()) -> ok | term()).
%% @doc
%% change the status of the stat in the metadata
%% @end
change_meta_status(Name, ToStatus) ->
  riak_stat_meta_mgr:change_status(Name, ToStatus).

-spec(unreg_meta_stat(StatName :: term()) -> term() | ok | {error, Reason :: term()}).
%% @doc
%% Marks the meta data of a stat as unregistered, deleting the stat from the
%% metadata will mean upon node restarting it will re_register. This option
%% prevents this from happening and keeps a record of the stats history
%% @end
unreg_meta_stat(Statname) ->
  riak_stat_meta_mgr:unregister(Statname).

-spec(reset_meta_stat(StatName :: term()) -> term() | ok | {error, Reason :: term()}).
%% @doc
%% Adds a count on to the metadata options that the stat has been updated
%% @end
reset_meta_stat(Statname) ->
  riak_stat_meta_mgr:reset_stat(Statname).

-spec(set_meta_opts(StatName :: term(), Opts :: list()) -> term() | ok | {error, Reason :: term()}).
%% @doc
%% Set the options (values) in the metadata manually
%% {status, Status}     -> Status :: enabled | disabled
%% {unregistered, Bool} -> Bool :: boolean()
%% @end
set_meta_opts(Statname, Opts) ->
  riak_stat_meta_mgr:set_options(Statname, Opts).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% EXOMETER %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec(register_exom_stats(StatName :: list(), Type :: atom(), Opts :: list(), Aliases :: list()) ->
  term() | ok | {error, Reason :: term()}).
%% @doc
%% register the stats in exometer
%% @end
register_exom_stats(Statname, Type, Opts, Aliases) ->
  riak_stat_exom_mgr:register_stat(Statname, Type, Opts, Aliases).

-spec(update_exom_stats(StatName :: atom() | list(), UpdateVal :: term(),
    Type :: atom() | term()) -> term() | ok | {error, Reason :: term()}).
update_exom_stats(StatName, UpdateVal, Type) ->
  update_exom_stats(StatName, UpdateVal, Type, []).

-spec(update_exom_stats(StatName :: atom() | list(), UpdateVal :: term(),
    Type :: atom() | term(), Opts :: list() | tuple()) -> ok | {error, Reason :: term()} | term()).
%% @doc
%% updates the stat in exometer, if the stat does not exist it will create the stat
%% @end
update_exom_stats(StatName, UpdateVal, Type, Opts) ->
  riak_stat_exom_mgr:update_or_create(StatName, UpdateVal, Type, Opts).

-spec(select_stat(Pattern :: term()) -> ok | term()).
%% @doc
%% find the stat from exometer with the selected pattern
%% @end
select_stat(Pattern) ->
  riak_stat_exom_mgr:select_stat(Pattern).

-spec(alias(Arg :: term()) -> term()).
%% @doc Used in riak_kv_status @end
alias(Arg) ->
  gen_server:cast(?SERVER, {alias, Arg}).

-spec(aliases(Type :: atom(), Args :: term()) -> ok | term()).
%% @doc
%% send to exometer_alias to perform as specific alias function on the
%% entries given
%% @end
aliases(Type, Args) ->
  gen_server:call(?SERVER, {aliases, Type, Args}).

-spec(check_exom_info(App :: atom() | term(), StatName :: term()) -> term() | ok | {error, Reason :: term()}).
%% @doc
%% looks in exometer for an items specific value and returns it
%% @end
check_exom_info(Name, Item) ->
  riak_stat_exom_mgr:info(Name, Item).

-spec(change_exom_status(Name :: list() | atom(), Status :: atom() | list()) -> ok | term()).
%% @doc
%% changes the options in exometer so the status is either disabled or enabled
%% will change in exometer if there is also a change in metadata
%% @end
change_exom_status(Name, Status) ->
  set_exom_opts(Name, [{status, Status}]).

-spec(unreg_exom_stat(StatName :: term()) -> term() | ok | {error, Reason :: term()}).
%% @doc
%% unregister the stat form exometer, after the stat is marked as unregistered in
%% metadata
%% @end
unreg_exom_stat(Statname) ->
  riak_stat_exom_mgr:unregister_stat(Statname).

-spec(reset_exom_stat(StatName :: term()) -> term() | ok | {error, Reason :: term()}).
%% @doc
%% Reset the stat in exometer back to default
%% @end
reset_exom_stat(Statname) ->
  riak_stat_exom_mgr:reset_stat(Statname).

% change the opts in exometer manually etc
-spec(set_exom_opts(StatName :: term(), Opts :: list()) -> term() | ok | {error, Reason :: term()}).
%% @doc
%% changes the options in exometer, including the status of the stat
%% @end
set_exom_opts(Statname, Opts) ->
  riak_stat_exom_mgr:set_opts(Statname, Opts).


%%%% TEST %%%%

start() ->
  riak_stat_exom_mgr:start().

stop() ->
  riak_stat_exom_mgr:stop().


%% Used as exometer_folsom in riak_core_exo_monitor

counter(Name) ->
  riak_stat_exom_mgr:new_counter(Name).

history(Name, Len) ->
  riak_stat_exom_mgr:new_history(Name, Len).

gauge(Name) ->
  riak_stat_exom_mgr:new_gauge(Name).

get_datapoints(Name, Type) ->
  riak_stat_exom_mgr:get_datapoints(Name, Type).
get_datapoints(Name, Type, Opts) ->
  riak_stat_exom_mgr:get_datapoints(Name, Type, Opts).

get_val_fol(Name, Type, Opts, DPs) ->
  riak_stat_exom_mgr:get_value_folsom(Name, Type, Opts, DPs).

notify(Name, Val, Type) ->
  riak_stat_exom_mgr:notify_metric(Name, Val, Type).

delete_metric(Arg) ->
  riak_stat_exom_mgr:delete_metric(Arg).


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

-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: #state{}) ->
  {reply, Reply :: term(), NewState :: #state{}} |
  {reply, Reply :: term(), NewState :: #state{}, timeout() | hibernate} |
  {noreply, NewState :: #state{}} |
  {noreply, NewState :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term(), Reply :: term(), NewState :: #state{}} |
  {stop, Reason :: term(), NewState :: #state{}}).

%%% GENERAL %%%

handle_call({register, App, Stats}, _From, State) ->
  Reply = lists:foreach(fun(Stat) ->
    register_stat(prefix(), App, Stat)
                end, Stats),
  {reply, Reply, State};

%% TODO: get rid of this function, it has been made redundant
handle_call({vnode_stats, App, Stats}, _From, State) ->
  lists:foreach(fun
                  ({Stat, Type, Alias}) ->
                    register_stats(App, [{Stat, Type, [], Alias}]);
                  ({Stat, Type}) ->
                    register_stats(App, [{Stat, Type}])
                end, Stats),
  {reply, ok, State};


handle_call({get_app_stats, App}, _From, State) ->
  Values = get_values([prefix(), App]),
  {reply, Values, State};
handle_call({update, App, Name, UpdateVal, Type}, _From, State) ->
  StatName = stat_name(prefix(), App, Name),
    case check_meta_info(StatName) of
      undefined ->
        register_meta_stats(StatName, Type, [], []),
        update_exom_stats(StatName, UpdateVal, Type);
      {{_NI, _S}, StatName, {Type, Opts, _Aliases}} ->
        update_exom_stats(StatName, UpdateVal, Type, Opts);
      _ ->
        ok
    end,
  {reply, ok, State};
handle_call({get_datapoint, Name, DataPoint}, _From, State) ->
  Value =
    riak_stat_exom_mgr:get_datapoint(Name, DataPoint),
  {reply, Value, State};
handle_call({unregister, Mod, Index, Type, App}, _From, State) ->
  unreg_stats(App, Type, Mod, Index),
  {reply, ok, State};
%%handle_call({method, Method}, _From, State = #state{}) ->
%%  Method0 = method(Method),
%%  {Response, Arg} =
%%    case Method0 of
%%      metadata -> {"Method changed to ~p as default~n", Method0};
%%      exometer -> {"Method changed to ~p as default~n", Method0};
%%      lww -> {"Method changed to ~p as default~n", Method0};
%%      _ -> {"Invalid method given~n", []}
%%  end,
%%  Reply = io:fwrite(Response, [Arg]),
%%  {reply, Reply, State#state{method = Arg}};

% CONSOLE %%%

handle_call({show, Arg, Status}, _From, State = #state{}) ->
  Reply = find_entries(Arg, Status),
  {reply, Reply, State};
handle_call({stat0, Arg}, _From, State) ->
  io:fwrite("Arg: ~p~n", [Arg]),
  not_updating(Arg),
%%  io:fwrite("NotUpdata: ~p Updating : ~p~n", [NotUpdating, _Updating]),
  {reply, ok, State};
handle_call({disable0, Arg}, _From, State) ->
  {NotUpdating, _Updating} = not_updating(Arg),
  [stat_change(Name, disabled) || {Name, _Val} <- NotUpdating],
  {reply, ok, State};
handle_call({info_stat, Arg}, _From, State) ->
  {reply, pick_info_attrs(split_arg(Arg)), State};
handle_call({change_status, _Arg, ToStatus}, _From, State) ->
  OldStatus = case ToStatus of
                enabled -> disabled;
                disabled -> enabled;
                _ -> '_'
              end,
  {reply, OldStatus, State};
%%handle_call({reset, Arg}, _From, State) ->
%%  responder(Arg, enabled, fun reset_stats/2, []),
%%  {reply, ok, State};


%%% EXOMETER %%%

handle_call({aliases, Type, Arg}, _From, State) ->
  riak_stat_exom_mgr:aliases(Type, Arg),
  {reply, ok, State};
handle_call({info, Name, Item}, _From, State) ->
  Info = check_exom_info(Name, Item),
  {reply, Info, State};

%%% PROFILES %%%

handle_call({load, ProfileName}, _From, State = #state{}) ->
  ProfName = sanitise(ProfileName),
  riak_stat_meta_mgr:load_profile(ProfName),
  {reply, ok, State#state{profile = ProfileName}};
handle_call(reset_profile, _From, State = #state{profile = ProfileName}) ->
  Stats = riak_stat_meta_mgr:reset_profile(ProfileName),
  change_these_stats(Stats),
  {reply, ok, State#state{profile = []}};
handle_call({add_profile, ProfileName0}, _From, State) ->
  ProfileName = sanitise(ProfileName0),
  Reply =
  case ensure_directory() of
    ok ->
      case read_file(ProfileName) of
        {error, Reason} ->
          io:fwrite("error: ~p~n", [Reason]);
        Data -> % data is returned in a string and needs sanitising
          register_meta_profiles(ProfileName, string_sanitiser(Data))
      end;
    {error, Reason} ->
      io:fwrite("error: ~p~n", [Reason])
  end,
  {reply, Reply, State};
handle_call({add_profile_stat, Stat}, _From, State = #state{profile = Profile}) ->
  SaniData = string_sanitiser(Stat),
  riak_stat_meta_mgr:add_profile_stat(Profile, SaniData),
  {reply, ok, State};
handle_call({remove_profile, ProfileName}, _From, State = #state{profile = CurrentProfile}) ->
  ProfName = sanitise(ProfileName),
  case ProfName of
    CurrentProfile ->
      reset_profiles(),
      remove_a_profile(ProfName);
    _ ->
      remove_a_profile(ProfName)
  end,
  {reply, ok, State};
handle_call({remove_profile_stat, Stat}, _From, State = #state{profile = Profile}) ->
  Stats = [StatName || {{StatName, _, _S}, _DP} <- find_entries(Stat, '_')],
  riak_stat_meta_mgr:remove_profile_stat(Profile, Stats),
  {reply, ok, State};
handle_call({change_profile_stat, Stat}, _From, State = #state{profile = Profile}) ->
  SaniData = string_sanitiser(Stat),
  riak_stat_meta_mgr:change_profile_stat(Profile, SaniData),
  {reply, ok, State};
handle_call({check_profile_stat, Stat}, _From, State = #state{profile = Profile}) ->
  Stats = [StatName || {{StatName, _, _S}, _DP} <- find_entries(Stat, '_')],
  Reply =
  [io:fwrite("~p: ~p~n", [Stat1, Status]) || [{Stat1, Status}]
  <- lists:map(fun(Stat0) ->
      riak_stat_meta_mgr:check_profile_stat(Profile, Stat0)
              end, Stats)],
  {reply, Reply, State};
handle_call(_Request, _From, State) ->
  {reply, ok, State}.


%%% -------------------------------------------------------------------

-spec(handle_cast(Request :: term(), State :: #state{}) ->
  {noreply, NewState :: #state{}} |
  {noreply, NewState :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #state{}}).

%%handle_cast({register, App, Stats}, State) ->
%%  lists:foreach(fun(Stat) ->
%%    register_stat(prefix(), App, Stat)
%%                end, Stats),
%%  {noreply, State};
%%handle_cast({vnode_stats, App, Stats}, State) ->
%%  lists:foreach(fun
%%                  ({Stat, Type, Alias}) ->
%%                    register_stats(App, [{Stat, Type, [], Alias}]);
%%                  ({Stat, Type}) ->
%%                    register_stats(App, [{Stat, Type}])
%%                end, Stats),
%%  {noreply, State};
%%handle_cast({get_app_stats, App}, State) ->
%%  Values = get_values([prefix(), App]),
%%  io:fwrite("~p~n", [Values]),
%%  {noreply, State};
%%handle_cast({update, App, Name, UpdateVal, Type}, State) ->
%%  StatName = stat_name(prefix(), App, Name),
%%  case check_meta_info(StatName) of
%%    undefined ->
%%      register_meta_stats(StatName, Type, [], []),
%%      update_exom_stats(StatName, UpdateVal, Type);
%%    {{_NI, _S}, StatName, {Type, Opts, _Aliases}} ->
%%      update_exom_stats(StatName, UpdateVal, Type, Opts);
%%    _ ->
%%      ok
%%  end,
%%  {noreply, State};
%%handle_cast({get_datapoint, Name, DataPoint}, State) ->
%%  Value =
%%    riak_stat_exom_mgr:get_datapoint(Name, DataPoint),
%%  io:fwrite("~p~n", [Value]),
%%  {noreply, State};
%%handle_cast({unregister, Mod, Index, Type, App}, State) ->
%%  unreg_stats(App, Type, Mod, Index),
%%  {noreply, State};
%%
%%
%%handle_cast({show, Arg, Status}, State = #state{}) ->
%%  Reply = print_stats(find_entries(Arg, Status), []),
%%  io:fwrite("~p~n", [Reply]),
%%  {noreply, State};
%%handle_cast({stat0, Arg}, State) ->
%%  {NotUpdating, _Updating} = not_updating(Arg),
%%  Reply = [io:fwrite("~p~n", [Name]) || {Name, _Val} <- NotUpdating],
%%  io:fwrite("~p~n", [Reply]),
%%  {noreply, State};
%%handle_cast({disable0, Arg}, State) ->
%%  {NotUpdating, _Updating} = not_updating(Arg),
%%  [stat_change(Name, disabled) || {Name, _Val} <- NotUpdating], % todo: stat changes the metadata too
%%  {noreply, State};
%%handle_cast({info_stat, Arg}, State) ->
%%  {Attrs, RestArg} = pick_info_attrs(split_arg(Arg)),
%%  [print_stats(E, Attrs) || E <- find_entries(RestArg, '_')],
%%  {noreply, State};
%%handle_cast({change_status, Arg, ToStatus}, State) ->
%%  OldStatus = case ToStatus of
%%                enabled -> disabled;
%%                disabled -> enabled;
%%                _ -> '_'
%%              end,
%%  responder(Arg, OldStatus, fun change_status/2, ToStatus),
%%  {noreply, State};
%%handle_cast({reset, Arg}, State) ->
%%  responder(Arg, enabled, fun reset_stats/2, []),
%%  {noreply, State};


handle_cast({alias, Arg}, State) ->
  riak_stat_exom_mgr:alias(Arg),
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

vnodeq_atom(Service, Desc) ->
  binary_to_atom(<<(atom_to_binary(Service, latin1))/binary, Desc/binary>>, latin1).

timestamp() ->
  riak_stat_exom_mgr:timestamp().

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
  % Pull out the status of the stat from MetaData
  NewOpts = register_meta_stats(StatName, Type, Opts, Aliases),
  register_exom_stats(StatName, Type, NewOpts, Aliases).

% Put the prefix and App name in front of every stat name.
stat_name(P, App, N) when is_atom(N) ->
  stat_name_([P, App, N]);
stat_name(P, App, N) when is_list(N) ->
  stat_name_([P, App | N]).

stat_name_([P, [] | Rest]) -> [P | Rest];
stat_name_(N) -> N.

%%%===================================================================
%%% Stat show/info
%%%===================================================================

print_stats(Entries, Attributes) ->
  riak_stat_assist_mgr:print_stats(Entries, Attributes).

find_entries(Arg, Status) ->
  riak_stat_assist_mgr:find_entries(Arg, Status).

split_arg([Str]) ->
  re:split(Str, "\\s", [{return, list}]).

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


%%%===================================================================
%%% change status
%%%===================================================================

%%change_status(N, St) ->
%%  change_status(core, N, St).

change_status(Name, ToStatus) ->
  case change_meta_status(Name, ToStatus) of
    ok ->
      change_exom_status(Name, ToStatus);
    {error, Reason} ->
      Reason;
    _ ->
      change_exom_status(Name, ToStatus)
  end.

change_these_stats(Stats) ->
  lists:map(fun({StatName, {status, Status}}) ->
    change_status(StatName, Status)
            end, Stats).

%%%===================================================================
%%% Update stats
%%%===================================================================

%% Todo: move this to the assitant manager, use the same function as
%% print_info_1 to get the value, and when the value = [] the send to the
%% normal print_stats with Attrs [status, value].

not_updating(Arg) ->
  {{Entries, _, _S}, _DPs} = find_entries(Arg, enabled),
  riak_stat_assist_mgr:print_stats0(Entries).
%%  io:fwrite("Entries ~p ~n", [Entries]),
%%  lists:foldl(fun
%%                ({Name, []}, {Nil, Ok}) -> {[{Name, []} | Nil], Ok};
%%                ({Name, 0}, {Nil, Ok}) -> {[{Name, []} | Nil], Ok};
%%                ({Name, undefined}, {Nil, Ok}) -> {[{Name, []} | Nil], Ok};
%%                ({Name, Val}, {Nil, Ok}) -> {Nil, [{Name, Val} | Ok]}
%%              end, {[], []},
%%    [{E, get_info(E, value)} || {{E, _, _S}, _DP} <- Entries]).

print(Arg) ->
  riak_stat_assist_mgr:just_print(Arg).

%%print(Arg, Status) ->
%%  riak_stat_assist_mgr:just_print(Arg, Status).

%%%===================================================================
%%% Delete/reset Stats
%%%===================================================================

unreg_stats(App, Type, [Op, time], Index) ->
  P = prefix(),
  unreg_from_both([P, App, Type, Op, time, Index]);
unreg_stats(App, Type, Mod, Index) ->
  P = prefix(),
  unreg_from_both([P, App, Type, Mod, Index]).

unreg_from_both(StatName) ->
  case unreg_meta_stat(StatName) of
    ok ->
      unreg_exom_stat(StatName);
    unregistered ->
      unreg_exom_stat(StatName);
    _ ->
      lager:debug("riak_stat_mngr:unreg_both -- could not unregister from meta~n"),
      ok
  end.

% Extra in the case of change status is the new status
% in the case of reset status it is - the incremental value ot reset it by
responder(Arg, CuStatus, Fun, Extra) ->
  lists:foreach(
    fun({[{LP, []}], _}) ->
      io:fwrite(
        "== ~s (Legacy pattern): No matching stats ==~n", [LP]);
      ({[{LP, Matches}], _}) ->
        io:fwrite("== ~s (Legacy pattern): ==~n", [LP]),
        [io:fwrite("~p: ~p~n", [N, Fun(N, Extra)])
          || {N, _} <- Matches];
      ({Entries, _}) ->
        [io:fwrite("~p: ~p~n", [N, Fun(N, Extra)])
          || {N, _, _} <- Entries]
    end, find_entries(Arg, CuStatus)).

%%%===================================================================
%%% Profiles
%%%===================================================================

ensure_directory() ->
  filelib:ensure_dir(profiles).

read_file(FileName) ->
  case open_file(FileName) of
    {ok, IoDevice} ->
      case read(IoDevice) of
        {ok, Data} ->
          Data;
        eof ->
          {error, no_data};
        {error, Reason} ->
          {error, Reason}
      end;
    {error, Reason} ->
      Reason
  end.

read(IoDevice) ->
  file:read(IoDevice, 666).

open_file("riak/doc/profiles/" ++ FileName) ->
  file:open(FileName, [raw]).

string_sanitiser(Data) ->
  case Data of
    "[" ++ Rest ->
      % the data has been stored in a list
      string_sanitiser_helper(Rest);
    Rest when is_list(Rest)->
      string_sanitiser_helper(Rest)
  end.

string_sanitiser_helper(Data) ->
  {RawToEnable0, RawToDisable0} = find(Data),
  RawToEnable = sanitise_me(RawToEnable0),
  RawToDisable = sanitise_me(RawToDisable0),
  {{RawEnEntries, _, _S}, _DP} =  [find_entries(Arg, '_') || Arg <- RawToEnable],
  {{RawDisEntries, _A, _SA}, _DPA} = [find_entries(Arg, '_') || Arg <- RawToDisable],
  the_alpha_stat(
  meta_keyer(RawEnEntries, enabled) , meta_keyer(RawDisEntries, disabled)).

find(Data) ->
  EnabledData = search_for(Data, enabled),
    case search_for(EnabledData, disabled) of
      nomatch ->
        case search_again_for(EnabledData, disabled) of
          nomatch ->
            {EnabledData, []};
          DisabledData ->
            {EnabledData, DisabledData}
        end;
      DisabledData ->
        {EnabledData, DisabledData}
    end.

find_enabled(String, Dir) ->
  find_(String, "{enabled,", Dir).

find_disabled(String, Dir) ->
  find_(String, "{disabled,", Dir).

find_(String, Search, Dir) ->
  string:find(String, Search, Dir).

search_for(String, enabled) ->
  find_enabled(String, trailing);
search_for(String, disabled) ->
  find_disabled(String, trailing).

search_again_for(String, enabled) ->
  find_enabled(String, leading);
search_again_for(String, disabled) ->
  find_disabled(String, leading).

sanitise([Data]) when is_list(Data) ->
  sanitise_(Data).

sanitise_(Data) when is_list(Data)->
  list_to_atom(Data).


sanitise_me(Data) ->
  case Data of
    [] ->
      [];
    "[" ++ Rest ->
      put_me_in_list(next(Rest));
    TheRest ->
      put_me_in_list(next(TheRest))
  end.

next(Rest) ->
  case find_(Rest, "}", trailing) of
    nomatch ->
      Rest;
    NewRest ->
      NewRest
  end.

put_me_in_list(Data) ->
  re:split(Data, ",", [{return, list}]).

meta_keyer(Keys, Status) ->
  lists:foldl(
    fun(StatName, Acc) ->
      [put_me_in_tuple(StatName, Status) | Acc]
    end, [], Keys).

put_me_in_tuple(Key, Value) ->
  {Key, {status, Value}}. % this is the full Value stored in the metadata,
                          % the profile name is the key

%% In the case when everything is disabled, which is most likely the case then
%% some stats will be saved more than once with disabled and enabled as values
%% this goes through the list and removes any that have both enabled and disabled
%% values, leaving the enabled stats behind
the_alpha_stat(Enabled, Disabled) ->
  % The keys are sorted first with ukeysort which deletes duplicates, then merged
  % so any key with the same stat name that is both enabled and disabled returns
  % only the enabled option.
  lists:ukeymerge(2, lists:ukeysort(1, Enabled), lists:ukeysort(1, Disabled)).
  % The stats must fight, to become the alpha

remove_a_profile(Profile) ->
  riak_stat_meta_mgr:remove_profile(Profile).
