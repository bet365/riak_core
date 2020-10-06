%%%-----------------------------------------------------------------------------
%%% @doc
%%% Profiles are the "saved setup of stat configuration", basically, if the
%%% current statuses of the stats are in a state you would like to repeat either
%%% for testing or default preferences they can be saved into the metadata and
%%% gossiped to all nodes. Thus allowing a profile for one node to be mimic'ed
%%% on all nodes in the cluster.
%%%
%%% This means - unfortunately, trying to save the nodes setup (if the state of
%%% the stats status is different on all or some nodes) at once with the same
%%% name, the LWW and the nodes individual setup could be overwritten, all
%%% setups are treated as a globally registered setup that can be enabled on one
%%% or all nodes in a cluster. therefore saving a clusters setup should be done
%%% on a per node basis.
%%%
%%% save-profile <entry> ->
%%%     Saves the current stats and their status as a value to the
%%%     key: <entry>, in the metadata
%%%
%%% load-profile <entry> ->
%%%     Loads a profile that is saved in the metadata with the key <entry>,
%%%     pulls the stats and their status out of the profiles metadata value
%%%
%%% load-profile-all <entry> ->
%%%     Same as the load profile but will load that profile on all
%%%     the nodes in the cluster.
%%%
%%% delete-profile <entry> ->
%%%     Delete a profile <entry> from the metadata, does not effect the
%%%     current configuration if it is the currently loaded profile
%%%
%%% reset-profile ->
%%%     unloads the current profile and changes all the stats back to
%%%     enabled, no entry needed. Does not delete any profiles
%%%
%%% pull_profiles() ->
%%%     Pulls the list of all the profiles saved in the metadata and
%%%     their [{Stat, {status, Status}}] list
%%%
%%% NOTE :
%%%   When creating a profile, you can make changes and then overwrite the
%%%   profile with the new stat configuration. However if the profile is saved
%%%   and then loaded, upon re-write the new profile is different but still
%%%   enabled as loaded, therefore it can not be loaded again until it is
%%%   deleted and created again, or the profile is reset
%%%
%%%     for example:
%%% Node1: stat save-profile test-profile
%%% -- saves the profile and the current setup in metadata --
%%%
%%% Node2: stat load-profile test-profile
%%% -- loads the profile that is saved, is recognised as the
%%%    current profile --
%%%
%%% Node1: stat disable stat.name.**
%%% Node1: stat save-profile test-profile
%%% -- changes the setup of the stats and rewrites the current
%%%     profile saved --
%%%
%%% Node2: stat load-profile test-profile
%%%     > "profile already loaded"
%%% -- even though the setup has changed this profile is currently
%%%    loaded in the old configuration, it will not change the stats
%%%    to the new save unless it is unloaded and reloaded again --
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(riak_stats_profiles).
-include("riak_stats.hrl").

%% API
-export([
    save_profile/1,         get_profile/1,
    load_profile/2,         get_all_profiles/0,
    delete_profile/1,       get_loaded_profile/1,
    reset_profile/1
]).

-define(PROFILE_PREFIX,           {profiles, list}).
-define(PROFILE_KEY(Profile),     {?PROFILE_PREFIX, Profile}).

-define(LOADED_PREFIX,            {profiles, loaded}).
-define(LOADED_PKEY,              {?LOADED_PREFIX, node()}).
-define(LOADED_PKEY(Key),         {?LOADED_PREFIX, Key}).

-type reason()          :: not_found | deleted | term().
-type response()        :: ok | {error, reason()} | list().

%% -----------------------------------------------------------------------------
%% @doc
%% Saves the profile name in the metadata with all the current stats and their
%% status as the value; multiple saves of the same name overwrites the profile
%% @end
%% -----------------------------------------------------------------------------
-spec(save_profile(profile_name()) -> {response(), profile_name()}).
save_profile(ProfileName) ->
    Stats = riak_stats_metadata:get_all_stats(),
    {riak_stats_metadata:put(?PROFILE_PREFIX,ProfileName,Stats), ProfileName}.

%%%-----------------------------------------------------------------------------
%% @doc
%% Find the profile in the metadata and pull out stats to change them. It will
%% compare the current stats with the profile stats and will change the ones
%% that need changing to prevent errors/less expense
%% @end
%%%-----------------------------------------------------------------------------
-spec(load_profile(profile_name(), [node()]) -> {response(), profile_name()}).
load_profile(ProfileName,Nodes) ->
    Response =
        case get_profile(ProfileName) of
            {error, Reason} -> {error, Reason};
            SavedStats ->
                CurrentStats = riak_stats_metadata:get_all_stats(),
                ToChange = SavedStats -- CurrentStats,
                %% delete stats that are already enabled/disabled, any
                %% duplicates with different statuses will be replaced
                %% with the profile one
                riak_stats:change_status(ToChange),
                lists:foreach(fun(Node) ->
                    riak_stats_metadata:put(?LOADED_PREFIX, Node, ProfileName)
                              end, Nodes),
                ok
            %% the reason a profile is not checked to see if it is already
            %% loaded is because it is easier to "reload" an already loaded
            %% profile in the case the stats configuration is changed,
            %% rather than "unloading" the profile and reloading it to
            %% change many stats statuses unnecessarily

            %% consequentially, save -> load -> change -> load again
            %% would mean no stats would change if the profile is already
            %% loaded
        end,
    {Response, ProfileName}.

%%%-----------------------------------------------------------------------------
%% @doc Find the profile in the metadata @end
%%%-----------------------------------------------------------------------------
-spec(get_profile(profile_name()) -> response()).
get_profile(ProfileName) ->
    ProfileKey = ?PROFILE_KEY(ProfileName),
    case riak_stats_metadata:check_meta(ProfileKey) of
        []            -> {error, not_found};
        unregistered  -> {error, deleted};
        ProfileValue  -> ProfileValue
    end.

%%%-----------------------------------------------------------------------------
%% @doc List of all the profiles in the metadata @end
%%%-----------------------------------------------------------------------------
-spec(get_all_profiles() -> response()).
get_all_profiles() -> riak_stats_metadata:get_all(?PROFILE_PREFIX).

%%%-----------------------------------------------------------------------------
%% @doc List of the profile currently loaded on the node @end
%%%-----------------------------------------------------------------------------
-spec(get_loaded_profile(node()) -> {profile_name(), node()}).
get_loaded_profile(Node) ->
    Profile = riak_stats_metadata:get(?LOADED_PREFIX, Node),
    {Profile, Node}.

%%%-----------------------------------------------------------------------------
%% @doc
%% deletes the profile from the metadata and all its values but it does not
%% affect the status of the stats.
%% @end
%%%-----------------------------------------------------------------------------
-spec(delete_profile(profile_name()) -> {response(), profile_name()}).
delete_profile(ProfileName) ->
    Response =
        case get_profile(ProfileName) of
            {error, Reason} -> {error, Reason};
            _  ->
                OnAllNodes = [node()|nodes()],
                %% unload the profile on all nodes if it is loaded.
                lists:foreach(fun(Node) ->
                    unload_profile(ProfileName, Node)
                              end, OnAllNodes),
                %% delete the Profile from the metadata
                riak_stats_metadata:delete(?PROFILE_PREFIX, ProfileName),
                ok
        end,
    {Response, ProfileName}.

unload_profile(ProfileName, Node) ->
    case riak_stats_metadata:check_meta(?LOADED_PKEY(Node)) of
        ProfileName ->
            riak_stats_metadata:put(?LOADED_PREFIX, Node, ["none"]);
        _ ->  %% profile is not loaded on that node.
            ok %% do nothing
    end.

%%%-----------------------------------------------------------------------------
%% @doc resets the profile and enable all the disabled stats. @end
%%%-----------------------------------------------------------------------------
-type reset_profiles() :: [{profile_name(), node()}] | [].
-spec(reset_profile([node()]) -> {ok, reset_profiles()}).
reset_profile(Nodes) ->
    %% get list of profiles unloaded and the nodes.
    Profiles =
        lists:foldl(fun(Node, ProfileAcc) ->
            case riak_stats_metadata:check_meta(?LOADED_PKEY(Node)) of
                ["none"]     -> ProfileAcc;
                []           -> ProfileAcc;
                unregistered -> ProfileAcc;
                Profile ->
                    riak_stats_metadata:put(?LOADED_PREFIX,Node,["none"]),
                    [{Profile,Node} | ProfileAcc]
            end
                    end, [], Nodes),

    %% get all the stats that are disabled and re-enable them
    CurrentStats = riak_stats_metadata:get_all_stats(),
    riak_stats:change_status(
        [{Stat,enabled} || {Stat,Status} <- CurrentStats, Status =/= enabled]),
    {ok, Profiles}.

