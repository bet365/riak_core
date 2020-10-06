%%%-----------------------------------------------------------------------------
%%% @doc
%%% Supervise the TCP and UDP gen_server children that push stats to
%%% and endpoint given.
%%% @end
%%%-----------------------------------------------------------------------------
-module(riak_stats_push_sup).
-include("riak_stats_push.hrl").

-behaviour(supervisor).

%% API
-export([
    start_link/0,
    start_server/2,
    stop_server/1,
    restart_children/1,
    stop_running_server/2
]).

%% Supervisor callbacks
-export([init/1]).

-define(STRATEGY,  one_for_one).
-define(INTENSITY, 1).
-define(PERIOD,    5).

-define(ATTEMPTS,  50).
-define(RESTART,   transient).
-define(SHUTDOWN,  5000).

-define(SERVER,    ?MODULE).

%%%=============================================================================
%%% API functions
%%%=============================================================================

%%------------------------------------------------------------------------------
-spec(start_link() ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%%-----------------------------------------------------------------------------
%% @doc Start up a gen server for the pushing of stats to an endpoint. @end
%%%-----------------------------------------------------------------------------
-spec(start_server(protocol(), sanitised_push()) -> pid()).
start_server(Protocol, Data) ->
    CHILD = child_spec(Data,Protocol),
    {Child, Pid} = start_child(CHILD),
    log_and_respond({Child, Pid}),
    Pid.

%%%-----------------------------------------------------------------------------
%% @doc
%% Stop the persisting of stats by terminating and deleting the server
%% pushing the stats to an endpoint
%% @end
%%%-----------------------------------------------------------------------------
-spec(stop_server([pid()] | [atom()] | list()) -> ok | no_return()).
stop_server(Child) when is_list(Child) ->
    ChildName = list_to_atom(Child),
    Terminate = supervisor:terminate_child(?MODULE, ChildName),
    Delete    = supervisor:delete_child(?MODULE, ChildName),
    log_and_respond({Child, {Terminate, Delete}}).

%%%=============================================================================
%%% Printing API
%%%=============================================================================

log_and_respond({Child, Response}) ->
    log(Child, Response),
    respond(Response),
    ok.

%% @doc second argument is tuple, only provided by stop_server/1 @end
log(Child, {ok, ok}) -> lager:info("Child Terminated and Deleted : ~p", [Child]);
log(Child, {ok, Error}) ->
    lager:info("Child Terminated : ~p",[Child]),
    lager:info("Error in Deleting ~p : ~p",[Child, Error]);
log(Child, {Error, _}) ->
    lager:info("Child ~p termination error : ~p",[Child, Error]);

%% @doc Responses for start_server/2 @end
log(Child, Pid) when is_pid(Pid)->
    lager:info("Child started : ~p with Pid : ~p",[Child, Pid]);
log(Child, attempts_failed) ->
    lager:info("Attempts to start ~p failed, Exceeded Attempt limit : ~p",
        [Child, ?ATTEMPTS]);
log(Child, econnrefused) ->
    lager:error("Child refused to start - Connection refused. Child : ~p",
        [Child]);
log(Child, Error) ->
    lager:error("Child refused to start - ~p. Child : ~p",[Error, Child]).

respond({ok,ok}) -> ok;
respond(Pid) when is_pid(Pid) ->
    io:fwrite("Polling Started~n");
respond(econnrefused) ->
    io:fwrite("Could not initiate Polling, Connection refused~n");
respond(Error) ->
    io:fwrite("Could not initiate Polling, ~p~n", [Error]).

%%%=============================================================================
%%% Supervisor callbacks
%%%=============================================================================

%%------------------------------------------------------------------------------
init([]) ->
    Strategy   = ?STRATEGY,
    Intensity  = ?INTENSITY,
    Period     = ?PERIOD,
    SupFlags = #{strategy   => Strategy,
                 intensity  => Intensity,
                 period     => Period},
    restart_children(),
    Children = get_children(),
    {ok, {SupFlags, Children}}.

%%%=============================================================================
%%% Internal functions
%%%=============================================================================

restart_children() ->
    %% Get the children that were still running on shutdown.
    Children = get_children(),
    spawn(riak_stats_push_sup, restart_children, [Children]),
    ok.

-type children()   :: [child()].
-type child()      :: supervisor:child_spec().
-spec(restart_children(children()) -> ok).
restart_children(Children) ->
    restart_children(Children, ?ATTEMPTS, 1).
restart_children([], _, _) -> ok;
restart_children(Children,0,_) ->
    [log(Child, attempts_failed) || #{id := Child} <- Children],
    Keys = make_key(Children),
    stop_running_server(Keys);
%% Every 5 attempts double the Delay between attempts
restart_children(Children, Attempts, Delay) when (Attempts rem 5) == 0 ->
    ToRestart = restarter(Children),
    restart_children(ToRestart, Attempts-1, Delay*2);
restart_children(Children, Attempts, Delay) ->
    ToRestart = restarter(Children),
    restart_children(ToRestart, Attempts-1, Delay).

%% @doc Starts the children, if connection is refused they
%% are removed from the list and marked as running => false @end
restarter(Children) ->
    lists:foldl(fun
                    ({Child, econnrefused}, Acc) ->
                        Key = make_key(Child),
                        stop_running_server(Key),
                        log_and_respond({Child, econnrefused}),
                        Acc;
                    ({Child, Pid}, Acc) when is_pid(Pid) ->
                        Key = make_key(Child),
                        store_in_meta(Key),
                        log_and_respond({Child, Pid}),
                        Acc;
                    ({Child, _Other}, Acc) ->
                        [Child | Acc]
                end, [], start_children(Children)).

make_key(#{id := Name, start:={_M, _F, [{_Data,Protocol}]}}) ->
    {Protocol, atom_to_list(Name)};
make_key(Children) when is_list(Children) ->
    [make_key(Child) || Child <- Children, is_map(Child)].

stop_running_server(Keys) when is_list(Keys)->
    [stop_running_server(Key) || Key <- Keys];
stop_running_server(Key) when is_tuple(Key)->
    stop_running_server(?PUSH_PREFIX, Key).
stop_running_server(Prefix, Key) ->
    Fun = set_running_false_fun(),
    riak_stats_metadata:put(Prefix, Key, Fun).

set_running_false_fun() ->
    fun
        (#{running := true} = MapValue) ->
            MapValue#{
                running     => false,
                pid         => undefined,
                modified_dt => calendar:universal_time()}
    end.

store_in_meta(Key) ->
    MapValues = riak_stats_metadata:get(?PUSH_PREFIX, Key),
    riak_stats_push:store_setup_info(Key, MapValues).

%%%-----------------------------------------------------------------------------
%% @doc
%% Retrieve the information stored in the metadata about any gen_servers
%% that may have been running before the node was stopped.
%% @end
%%%-----------------------------------------------------------------------------
-spec(get_children() -> [child()]).
get_children() ->
    ListOfKids =
        riak_stats_push:fold_through_meta('_', {{'_', '_', '_'}, '_'},
            [node()]),
    lists:foldl(
        fun
            ({{Protocol,Instance}, #{running   := true,
                                     port      := Port,
                                     server_ip := Sip,
                                     stats     := Stats}}, Acc) ->
                Data = {{Port, Instance, Sip}, Stats},
                [child_spec(Data, Protocol) | Acc];
            (_other,Acc) -> Acc
        end, [], ListOfKids).

%%%-----------------------------------------------------------------------------
%% @doc Create a child spec out of the information given. @end
%%%-----------------------------------------------------------------------------
-spec(child_spec(sanitised_push(), protocol()) -> child()).
child_spec(Data, Protocol) ->
    ChildName = server_name(Data),
    Module    = riak_stats_push_server,
    Function  = start_link,
    Args      = {Data, Protocol},
    Restart   = ?RESTART,
    Shutdown  = ?SHUTDOWN,
    Type      = worker,
    MFArgs    = {Module, Function, [Args]},
    #{id      => ChildName,
     start    => MFArgs,
     restart  => Restart,
     shutdown => Shutdown,
     type     => Type,
     modules  => [Module]}.

server_name({{_, ServerName, _}, _}) -> list_to_atom(ServerName).

%%%-----------------------------------------------------------------------------
%% @doc
%% Start up the gen_server responsible for pushing stats and their values
%% to an endpoint. Passing in the Data needed.
%% @end
%%%-----------------------------------------------------------------------------
-spec(start_children(children()) -> [{child(), pid() | term()}]).
start_children(Children) ->
    [start_child(Child) || Child <- Children].

-spec(start_child(child()) -> {child(), pid() | term()}).
start_child(Child) ->
    case supervisor:start_child(?MODULE, Child) of
        {ok, Pid}                       -> {Child, Pid};
        {error, {already_started, Pid}} -> {Child, Pid};
        {error, econnrefused}           -> {Child, econnrefused};
        {error, {error, Reason}}        -> {Child, Reason};
        {error, Reason}                 -> {Child, Reason}
    end.