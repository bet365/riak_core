%%%-------------------------------------------------------------------
%%% @author dylanmiteloidentify
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 11. Jun 2019 11:11
%%%-------------------------------------------------------------------
-module(riak_core_metadata_events).
-author("dylanmitelo").

-behaviour(gen_event).

%% API
-export([start_link/0,
	add_handler/2,
	add_sup_handler/2,
	add_guarded_handler/2,
	add_callback/1,
	add_sup_callback/1,
	add_guarded_callback/1,
	metadata_update/2
]).

%% gen_event callbacks
-export([init/1, handle_event/2, handle_call/2,
	handle_info/2, terminate/2, code_change/3]).

-record(state, { callback }).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
	gen_event:start_link({local, ?MODULE}).

add_handler(Handler, Args) ->
	gen_event:add_handler(?MODULE, Handler, Args).

add_sup_handler(Handler, Args) ->
	gen_event:add_sup_handler(?MODULE, Handler, Args).

add_guarded_handler(Handler, Args) ->
	riak_core:add_guarded_event_handler(?MODULE, Handler, Args).

add_callback(Fn) when is_function(Fn) ->
	gen_event:add_handler(?MODULE, {?MODULE, make_ref()}, [Fn]).

add_sup_callback(Fn) when is_function(Fn) ->
	gen_event:add_sup_handler(?MODULE, {?MODULE, make_ref()}, [Fn]).

add_guarded_callback(Fn) when is_function(Fn) ->
	riak_core:add_guarded_event_handler(?MODULE, {?MODULE, make_ref()}, [Fn]).

metadata_update(Key, Metadata) ->
	gen_event:notify(?MODULE, {metadata_update, {Key, Metadata}}).

%% ===================================================================
%% gen_event callbacks
%% ===================================================================

init([Fn]) ->
%%	{ok, Ring} = riak_core_ring_manager:get_my_ring(),
%%	Fn(Ring),
	{ok, #state { callback = Fn }}.

handle_event({metadata_update, MetaData}, State) ->
	lager:info("Firing Event to vnode"),
	(State#state.callback)(MetaData),
	{ok, State}.


handle_call(_Request, State) ->
	{ok, ok, State}.

handle_info(_Info, State) ->
	{ok, State}.

terminate(_Reason, _State) ->
	ok.

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

