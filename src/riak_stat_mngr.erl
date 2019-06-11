%%%-------------------------------------------------------------------
%%% @copyright (C) 2019, Bet365
%%% @doc
%%%
%%% All stats modules and riak_core_console call into this manager to
%%% communicate with exometer and the data.
%%%
%%% All the stats are saved in the metadata, when the node starts up
%%% it checks the metadata first for which stats to register into
%%% exometer
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

%% API riak_core_console
-export([show_stat/1, show_stat/2,
  get_stats/2, get_stat/1, stat_info/1,
  info/2, change_status/3, stat_change/2, delete_stat/1, new_register/3,

  register_stat/3, register_stats/2, register_vnode_stats/1,
  alias/1, aliases/2, get_values/1, get_value/1,
  reset_stat/1, reset_stat/2, stat_opts/2,
  unregister_stats/4, vnodeq_atom/2,
  update_stats/3, show_pats/1, update/4,
  prefix/0, get_datapoints/2, get_datapoints/3, get_val_fol/4,
  get_fol_val/1, notify/3,
  start/0, stop/0,
  delete_metric/1, counter/1, history/2, gauge/1]).

-export([start_link/0]).

%% gen_server callbacks
-export([init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3]).

% TODO: merge some of the functions below
% TODO: below

-define(SERVER, ?MODULE).
-define(STAT, stats).

-record(state, {
  method = lww,     % lww is the default
  priority = meta,  % meta or exom
  aggr = off}).     % default decision for stats, turn aggregation off.
% some stats are aggregated to make it easier for the
% user to use

%%%===================================================================
%%% API
%%%===================================================================

%% TODO: Determine which functions require a reply (ok or otherwise)
%% and decide whether they should be gen_server:cast or call.

%% TODO: Consolidate all functions that have a similar input or output

%% TODO: Data entry needs to be generic, any data that goes into exometer
%% needs to be registered in metadata in a similar way. although the
%% metadata does not need constant updating, that is something specific
%% to exometer

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% CONSOLE %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @spec show_stat(Arg :: term()) -> term().
%% Default for show_stat(Arg) is enabled
show_stat(Arg) ->
  show_stat(Arg, enabled).
%% are we reading the data from exometer? or metadata, preferably
%% the metadata as all the exometer options are dependant on the metadata

%% Show disabled stats
show_stat(Arg, Status) ->
  gen_server:cast(?SERVER, {show, Arg, Status}).

%% the ability to see the stats that are disabled is easier to see how
%% many stats are not necessary or outdated. the vclocks on the stats
%% will show how old they are and if a stat is not used in a long time
%% it can be removed.

%% TODO: talk about where we want to keep the stats?
%% TODO: putting the stats themselves in the config could help with removing
%% TODO: and adding stats in future without having to update the code.
%% registering a stat for that specific module will be pulling the stats
%% out of the config which can be changed, if that config itself is
%% kept in the metadata. then when a stat needs to be added it can be
%% done so in a function right here. or removed, it will then be
%% persisted, removed forever? or put in the recycle bin?

%% TODO: talk to coxxy and dine about putting stats into config.
%% if they are removed from config that is stored down to disk they
%% arent completely lost as riak can be installed again.

%% or instead of putting the stats in config the names can be stored
%% in caching? then they can be removed from caching?
%% however then there would need to be functionality that the
%% caching will also have to read from the metadata which stats to cache.


%TODO: SPEC
get_stats(Name, DataPoint) ->
  gen_server:cast(?SERVER, {get, Name, DataPoint}).

stat_info(Arg) ->
  {Attrs, RestArg} = pick_info_attrs(split_arg(Arg)),
  [print_info(E, Attrs) || E <- find_entries(RestArg, '_')].

%% TODO: consolidate these functions, they can be input in a
%% similar way and it will reduce code expense

% TODO: SPEC
info(Name, Info) ->
  gen_server:cast(?SERVER, {info, Name, Info}).

%% TODO: function to enable/disable specific stat types (by mod name)
stat_change(Arg, ToStatus) ->
  gen_server:cast(?SERVER, {change_status, Arg, ToStatus}).
%% change it in the metadata first, this should be a gen_server call
%% as it will require a response to say it has done.


reset_stat(Arg) ->
  gen_server:cast(?SERVER, {reset, Arg}).
%% TODO: -spec / :: () -> .
%% TODO: a function for resetting stats in the metadata
reset_stat(_Mod, N) ->
  %% TODO: do gen_server call, fo exom and meta
  %% TODO: also update metadata with number of resets
  riak_stat_exom_mgr:reset(N).
%% Update the metadata when a stat is reset
%% A stat is reset either manually, or when a node is restarted and
%% re-registers the stat, it will inadvertantly keep track of the times
%% a node has been restarted,
%%
%% however if the node goes down continuously all the stats will have the
%% reset counter updated on resets continuously
%%
%% If instead the number of resets when the node restarts, it acts as
%% a stat itself, and it doesn't store the same number 500 times in the
%% metadata.


delete_stat(Arg) ->
  gen_server:cast(?SERVER, {delete, Arg}). % TODO: write the function

%% completely erase it from the metadata and the exometer,
%% maybe store in cache the stats that have been deleted, as they can be
%% removed from the code if they are no longer useful or required

% TODO: spec, "method.lww", "priority.metadata"
stat_opts(Name, Item) ->
  % metadata -> meta,
  % exometer -> exom
  gen_server:cast(?SERVER, {options, Name, Item}).
%% lww will compare the vclocks and then keep the youngest option,
%% the other option is metadata | exometer, as it will then choose which
%% stat to keep based on priority


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% STORING %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

register_stats(App, Stats) ->
  gen_server:call(?SERVER, {register, App, Stats}).

update_stats(App, Name, Arg) ->
  gen_server:call(?SERVER, {update, App, Name, Arg}).

new_register(App, Name, Arg) ->
  gen_server:call(?SERVER, {new_stat, App, Name, Arg}).

update(App, Name, UpdateVal, Type) ->
  gen_server:call(?SERVER, {update, App, Name, UpdateVal, Type}).

get_stat(App) ->
  gen_server:cast(?SERVER, {get, App}).

%%%% METADATA %%%%

%% TODO: check meta
%% TODO: put meta
%% TODO: get meta
%% TODO: delete meta
%% TODO: update reset counter
%% TODO: reset stat metadata
%% TODO: set meta opts


%%%% TEST %%%%

start() ->
  riak_stat_exom_mgr:start().

stop() ->
  riak_stat_exom_mgr:stop().


%TODO SPEC
register_vnode_stats(Stats) ->
  gen_server:cast(?SERVER, {vnode_stats, Stats}).

%TODO: SPEC
alias(Arg) ->
  gen_server:cast(?SERVER, {alias, Arg}).
%TODO: SPEC
aliases(Type, Args) ->
  gen_server:cast(?SERVER, {aliases, Type, Args}).

get_value(Stat) ->
  riak_stat_exom_mgr:get_value(Stat).

%TODO: SPEC
get_values(Path) ->
  gen_server:cast(?SERVER, {path, Path}).

% TODO: SPec
unregister_stats(Module, Index, Type, App) ->
  gen_server:cast(?SERVER, {unregister, Module, Index, Type, App}).

% tODO: spec
vnodeq_atom(Service, Desc) ->
  binary_to_atom(<<(atom_to_binary(Service, latin1))/binary, Desc/binary>>, latin1).


%TOdo: Spec
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
  {ok, #state{}}.

-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: #state{}) ->
  {reply, Reply :: term(), NewState :: #state{}} |
  {reply, Reply :: term(), NewState :: #state{}, timeout() | hibernate} |
  {noreply, NewState :: #state{}} |
  {noreply, NewState :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term(), Reply :: term(), NewState :: #state{}} |
  {stop, Reason :: term(), NewState :: #state{}}).
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


-spec(handle_cast(Request :: term(), State :: #state{}) ->
  {noreply, NewState :: #state{}} |
  {noreply, NewState :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #state{}}).

handle_cast({show, Arg, Status}, State = #state{priority = Priority}) ->
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
handle_cast({change_status, Arg, ToStatus}, State = #state{priority = Pr}) ->
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
handle_cast({options, Name, Item}, State = #state{method = Method, priority = Priority}) ->
  NewState =
    case Name of
      method when Item =/= Method, Item == lww; Item == priority ->
        State#state{method = Item};
      method when Item == Method ->
        lager:warning("method already chosen~n");
      priority when Item =/= Priority ->
        NewP =
          case Item of
            metadata -> meta;
            meta -> meta;
            exometer -> exom;
            exom -> exom;
            _ ->
              lager:warning("invalid entry~n")
          end,
        State#state{priority = NewP};
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
  Info = riak_stat_meta_mgr:info_stat(core, N),
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