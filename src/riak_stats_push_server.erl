%%%-----------------------------------------------------------------------------
%%% @doc Server to push stats to an endpoint via UDP or TCP @end
%%%-----------------------------------------------------------------------------
-module(riak_stats_push_server).
-include("riak_stats.hrl").
-include("riak_stats_push.hrl").
-behaviour(gen_server).

%% API
-export([start_link/1]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-define(SERVER, ?MODULE).

-record(state, {socket      :: socket(),
                port        :: port(),
                host        :: host(),
                instance    :: instance(),
                stats       :: stats(),
                protocol    :: protocol()}).

%%%===================================================================
%%% API
%%%===================================================================
-spec(start_link(Arg :: [{sanitised_push(),protocol()}]) ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link(Obj) ->
    gen_server:start_link(?MODULE, Obj, []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} | {stop, Reason}
%% @end
%%--------------------------------------------------------------------
-spec(init(Args :: term()) ->
    {ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term()} | ignore).
init({Info, Protocol}) ->
    case open(Protocol, Info) of
        {ok, Socket} ->
            State = create_state(Protocol, Socket, Info),
            schedule_push_stats(),
            {ok, State};
        {error, Error} ->
            lager:error("Error starting ~p because of : ~p",[?MODULE,Error]),
            {stop, Error}
    end.

%%------------------------------------------------------------------------------
-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: #state{}) ->
    {reply, Reply :: term(), NewState :: #state{}} |
    {reply, Reply :: term(), NewState :: #state{},timeout() | hibernate} |
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), Reply :: term(), NewState :: #state{}} |
    {stop, Reason :: term(), NewState :: #state{}}).
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

%%------------------------------------------------------------------------------
-spec(handle_cast(Request :: term(), State :: #state{}) ->
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #state{}}).
handle_cast(_Request, State) ->
    {noreply, State}.

%%------------------------------------------------------------------------------
-spec(handle_info(Info :: timeout() | term(), State :: #state{}) ->
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #state{}}).
handle_info(push_stats, #state{socket       = Socket,
                               port         = Port,
                               stats        = Stats,
                               host         = Host,
                               protocol     = Protocol} = State) ->
    push_stats(Protocol, {Socket, Host, Port, Stats}),
    schedule_push_stats(),
    {noreply, State};
handle_info(tcp_closed,State) ->
    {stop, endpoint_closed, State};
handle_info(_Info, State) ->
    {noreply, State}.

%%------------------------------------------------------------------------------
-spec(terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: #state{}) -> term()).
terminate(endpoint_closed, #state{instance = Instance, protocol = Protocol}) ->
    lager:error("Connection Closed on other end, terminating : ~p",[Instance]),
    terminate_server({Protocol, Instance}),
    ok;
terminate(shutdown, #state{instance = Instance, protocol = Protocol}) ->
    lager:info("Stopping ~p",[Instance]),
    terminate_server({Protocol, Instance}),
    ok;
terminate(_Reason, _State) ->
    ok.

%%------------------------------------------------------------------------------
-spec(code_change(OldVsn :: term() | {down, term()}, State :: #state{},
    Extra :: term()) ->
    {ok, NewState :: #state{}} | {error, Reason :: term()}).
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%=============================================================================
%%% Internal functions
%%%=============================================================================
open(udp, {{Port, _Instance, _Host}, _Stats})->
    Options = ?OPTIONS,
    gen_udp:open(Port, Options);
open(tcp, {{Port, _Instance, Host}, _Stats}) ->
    Options = ?OPTIONS,
    gen_tcp:connect(Host,Port,Options).

create_state(Protocol, Socket, {{MonitorLatencyPort, Instance, Host}, Stats}) ->
    #state{socket        = Socket,
           port          = MonitorLatencyPort,
           host          = Host,
           instance      = Instance,
           stats         = Stats,
           protocol      = Protocol}.

%%------------------------------------------------------------------------------
%% @doc Register the server as running => false. @end
%%------------------------------------------------------------------------------
terminate_server(Key) ->
    riak_stats_push_sup:stop_running_server(?PUSH_PREFIX, Key).

%%------------------------------------------------------------------------------

send(Socket, Data) ->
    gen_tcp:send(Socket, Data).

send(Socket, Host, Port, Data) ->
    gen_udp:send(Socket, Host, Port, Data).

send_after(Interval, Arg) -> erlang:send_after(Interval,self(),Arg).

%%------------------------------------------------------------------------------
%% @doc
%% Retrieve the stats from exometer and convert to json object, to
%% send to the endpoint. Repeat.
%% @end
%%------------------------------------------------------------------------------
push_stats(udp, {Socket, Host, Port, Stats}) ->
    JsonStats = riak_stats_push_util:json_stats(Stats),
    send(Socket, Host, Port, JsonStats);
push_stats(tcp, {Socket, _Host, _Port, Stats}) ->
    JsonStats = riak_stats_push_util:json_stats(Stats),
    send(Socket, JsonStats).

schedule_push_stats() ->
    send_after(?STATS_UPDATE_INTERVAL, push_stats).