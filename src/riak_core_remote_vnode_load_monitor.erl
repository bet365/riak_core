-module(riak_core_remote_vnode_load_monitor).

-behaviour(gen_server).

%% API
-export(
[
    start_link/1,
    reset/0,
    update_responsiveness_measurement/4

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
    name,
    table
}).

%%%===================================================================
%%% API
%%%===================================================================
start_link(Name) ->
    gen_server:start_link({local, Name}, ?MODULE, [Name], []).

reset() ->
    gen_server:cast(?SERVER, reset).

update_responsiveness_measurement(Code, Idx, StartTime, Endtime) ->
    gen_server:cast(Code, {update, Idx, StartTime, Endtime}).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
init([Name]) ->
    TableName = list_to_atom(atom_to_list(Name) ++ "_table"),
    Table = ets:new(TableName, [named_table, protected, ordered_set]),
    {ok, #state{table = Table, name = Name}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(reset, State) ->
    {noreply, State};

handle_cast({update, Idx, T0, T1}, State=#state{table = Tab, name = Name}) ->
    Diff = timer:now_diff(T1, T0),
    ets:insert(Tab, {Idx, Diff}),
    lager:info("remote_load_monitor ~p -> diff ~p; Idx ~p", [Name, Diff, Idx]),
    {noreply, State};

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