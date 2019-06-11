%%%-------------------------------------------------------------------
%%% @copyright (C) 2019, Bet365
%%% @doc
%%%
%%% June 2019 update -> cache system from riak_core_stat_cache moved
%%% into this module. Any functions to move into exometer or metadata
%%% will be made in this module.
%%%
%%% Be aware that exometer has it's own caching system, maybe be useful
%%% for implementation on all stats?
%%%
%%% TODO: Implement caching for all stats? as enabling and disabling
%%% occurs all at once, pulling it from cache might be quicker.
%%%
%%% @end
%%% Created : 05. Jun 2019 16:18
%%%-------------------------------------------------------------------
-module(riak_stat_cache_mgr).
-author("savannahallsop").

%% API
-export([]).


%% TODO: riak_core_stat_cache stores {M,F,A} in a list in state to
%% TODO: find the module and function that will produce the stats. it
%% TODO: is not the cache you are looking for.
%%
%% This cache manager is for data that will be stored in order to find
%% the status of the stats.
%%
%% basically all the "cache" data in the other modules will be kept in
%% this module instead of riak_core_stat_cache.
