%% -------------------------------------------------------------------
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
-module(riak_core_location).

%% API
-export([has_staged_location_change/1, clear_staged_locations/1,
         get_node_location/2]).

%% checking for staged location changes
-spec has_staged_location_change(riak_core_ring:riak_core_ring()) -> boolean().
has_staged_location_change(Ring) ->
  Locations = dict:to_list(riak_core_ring:get_nodes_locations(Ring)),
  [] =/= lists:filter(fun({_, {staged, _}}) -> true; (_) -> false end, Locations).

-spec clear_staged_locations(riak_core_ring:riak_core_ring()) ->
  riak_core:riak_core_ring().
clear_staged_locations(ChStatge) ->
  NodesLocations = riak_core_ring:get_nodes_locations(ChStatge),
  dict:fold(fun(Node, {staged, Location}, Acc) ->
                  riak_core_ring:set_node_location(Node, Location, Acc);
               (_, _, Acc) ->
                  Acc
            end, ChStatge, NodesLocations).

-spec get_node_location(node(), dict:dict()) -> binary() | unknown.
get_node_location(Node, Locations) ->
  case dict:find(Node, Locations) of
    error ->
      unknown;
    {ok, Location} ->
      Location
  end.
