%%%-------------------------------------------------------------------
%%% @author nordinesaabouni
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 07. Jun 2018 16:03
%%%-------------------------------------------------------------------
-module(riak_core_index_owner_watcher).
-author("nordinesaabouni").

-behaviour(gen_server).

%% API
-export(
[
    start_link/0,
    ring_update/1

]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {
    all_owners
}).

%%%===================================================================
%%% API
%%%===================================================================
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

ring_update(Ring) ->
    gen_server:cast(?SERVER, {ring_update, Ring}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
init([]) ->
    riak_core_ring_events:add_sup_callback(fun ?MODULE:ring_update/1),
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    AllOwners = riak_core_ring:all_owners(Ring),
    {ok, #state{all_owners = AllOwners}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({ring_update, Ring}, State = #state{all_owners = OldAllOwners}) ->
    NewAllOwners =  riak_core_ring:all_owners(Ring),
    case NewAllOwners == OldAllOwners of
        true ->
            {noreply, State};
        false ->
            AllIndexes = [Index || {Index, _Node} <- OldAllOwners],
            [riak_core_remote_vnode_load_monitor:reset(Index) || Index <- AllIndexes],
            {noreply, State#state{all_owners = NewAllOwners}}
    end;

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================


%%            TransferredIndexes = NewAllOwners -- OldAllOwners,
%%            DeletedIndexes = OldAllOwners -- NewAllOwners,
%%            NodesWithTransfers = lists:usort([Node || {_Index, Node} <- TransferredIndexes]),
%%            NodesWithDeletetions = lists:usort([Node || {_Index, Node} <- DeletedIndexes]),