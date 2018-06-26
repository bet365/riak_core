-module(riak_core_remote_vnode_load_monitor_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).


%%%===================================================================
%%% API functions
%%%===================================================================
start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%get_all_indexes() ->
%%    [Index || {Index, _Pid, _Type, Module} <- supervisor:which_children(?MODULE), Module == [riak_core_vnode_load_monitor]].

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================
init([]) ->
    RestartStrategy = one_for_one,
    MaxRestarts = 1000,
    MaxSecondsBetweenRestarts = 3600,
    SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},
    Restart = permanent,
    Shutdown = 2000,
    Type = worker,

    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    AllOwners = riak_core_ring:all_owners(Ring),
    AllIndexes = [Index || {Index, _Node} <- AllOwners],

    Children0 =
        [
            {'riak_core_index_owner_watcher', {'riak_core_index_owner_watcher', start_link, []}, Restart, Shutdown, Type, ['riak_core_index_owner_watcher']},
            {'riak_core_apl_blacklist', {'riak_core_apl_blacklist', start_link, []}, Restart, Shutdown, Type, ['riak_core_apl_blacklist']}
        ],

    Children1 =
        [
            {list_to_atom(integer_to_list(Index)), {'riak_core_remote_vnode_load_monitor', start_link, [Index]}, Restart, Shutdown, Type, ['riak_core_remote_vnode_load_monitor']}
            || Index <- AllIndexes
        ],

    Children = Children0 ++ Children1,

    {ok, {SupFlags, Children}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================