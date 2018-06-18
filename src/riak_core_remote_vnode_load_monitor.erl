-module(riak_core_remote_vnode_load_monitor).

-behaviour(gen_server).

%% API
-export(
[
    start_link/1,
    reset/1,
    update_responsiveness_measurement/5

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

    %% This is the partition index
    index,
    n_val_maximum,      % we use this value to determine when to drop the first n_val_2 data points for the average
    half_n_val_maximum, % when N reaches this value we begin calculations for the second half of the data points
    request_response_pairs % dictionary of dictionaries



}).

%%%===================================================================
%%% API
%%%===================================================================
start_link(Index) ->
    Name = list_to_atom(integer_to_list(Index)),
    gen_server:start_link({local, Name}, ?MODULE, [Index], []).

reset(Idx) ->
    gen_server:cast(list_to_atom(integer_to_list(Idx)), reset).

update_responsiveness_measurement(passed, Code, Idx, StartTime, Endtime) ->
    gen_server:cast(list_to_atom(integer_to_list(Idx)), {update_passed, Code, StartTime, Endtime});
update_responsiveness_measurement(failed, Code, Idx, StartTime, Endtime) ->
    gen_server:cast(list_to_atom(integer_to_list(Idx)), {update_failed, Code, StartTime, Endtime}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
init([Index]) ->
    NMax = app_helper:get_env(riak_core, responseiveness_n, 10000),
    HalfNMax2 = NMax div 2,
    State = #state{
        index = Index,
        n_val_maximum = NMax,
        half_n_val_maximum = HalfNMax2,
        request_response_pairs = dict:new()
    },
    {ok, State}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(reset, State) ->
    NMax = app_helper:get_env(riak_core, responseiveness_n, 10000),
    HalfNMax2 = NMax div 2,
    ResetState = #state{
        index = State#state.index,
        n_val_maximum = NMax,
        half_n_val_maximum = HalfNMax2,
        request_response_pairs = dict:new()
    },
    {noreply, ResetState};





handle_cast({update_passed, Code, T0, T1}, State=#state{request_response_pairs = Dict}) ->
    Diff = timer:now_diff(T1, T0),
    case dict:find(Code, Dict) of
        error ->
            CodeDict0 = make_new_code_dictionary(),
            NewState = update_distributions(passed, CodeDict0, Diff, State),
            {noreply, NewState};
        CodeDict0 ->
            _ = maybe_blacklist_vnode(passed, Code, Diff, State),
            NewState = update_distributions(passed, CodeDict0, Diff, State),
            {noreply, NewState}
    end;

handle_cast({update_failed, Code, T0, T1}, State=#state{request_response_pairs = Dict}) ->
    Diff = timer:now_diff(T1, T0),
    case dict:find(Code, Dict) of
        error ->
            CodeDict0 = make_new_code_dictionary(),
            CodeDict1 = add_timing_calculations_to_code_dictioanry(failed, CodeDict0, Diff, State),
            {noreply, State#state{request_response_pairs = dict:store(Code, CodeDict1, Dict)}};
        CodeDictionary0 ->
            CodeDictionary1 = add_timing_calculations_to_code_dictioanry(failed, CodeDictionary0, Diff, State),
            {noreply, State#state{request_response_pairs = dict:store(Code, CodeDictionary1, Dict)}}
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

make_new_code_dictionary() ->
    D = dict:new(),
    Avg = 0,
    Var = 0,
    Std = 0,
    N = 0,
    AvgCumFreq = 0,
    VarCumFreq = 0,
    Distribution = {Avg, Var, Std, N, AvgCumFreq, VarCumFreq},
    D1 = dict:store(distribution_1, Distribution, D),
    D2 = dict:store(distribution_2, Distribution, D1),
    D2.


maybe_blacklist_vnode(passed, _Code, _Diff, _Dict) ->
    % deviation = (diff - mean) / std
    % we will use this measurement and a set threshold to determine whether or not to forward on  the information
    % over to riak_core_apl_blacklist
    ok;
maybe_blacklist_vnode(failed, _Code, _Diff, _Dict) ->
    % the rules here will be slightly different as we will be saving different information
    ok.

move_distributions(Dict) ->
    {ok, Dis2} = dict:find(distribution_2, Dict),
    Avg = 0,
    Var = 0,
    Std = 0,
    N = 0,
    AvgCumFreq = 0,
    VarCumFreq = 0,
    Distribution = {Avg, Var, Std, N, AvgCumFreq, VarCumFreq},
    D1 = dict:store(distribution_1, Dis2, Dict),
    D2 = dict:store(distribution_2, Distribution, D1),
    D2.



update_distributions(passed, Dict, Diff, State = #state{n_val_maximum = Max, half_n_val_maximum = HalfMax}) ->
    {ok, Dis1} = dict:find(distribution_1, Dict),
    {_, _, _, N, _, _} = Dis1,
    case {N == Max, N < HalfMax} of
        {true, _} ->
            NewDict0 = move_distributions(Dict),
            NewDict1 = calculate_new_distribution(passed, 1, NewDict0, Diff),
            State#state{request_response_pairs = NewDict1};
        {false, true} ->
            % only calculate distribtuion 1
            NewDict = calculate_new_distribution(passed, 1, Dict, Diff),
            State#state{request_response_pairs = NewDict};
        {false, false} ->
            % calculate both distributions
            NewDict = calculate_new_distribution(passed, 2, Dict, Diff),
            State#state{request_response_pairs = NewDict}
    end;

update_distributions(failed, _Dict, _Diff, State) ->
    State.



calculate_new_distribution(passed, 1, Dict, Diff) ->
    ok;
calculate_new_distribution(passed, 2, Dict, Diff) ->
    ok;

calculate_new_distribution(failed, 1, Dict, Diff) ->
    ok;
calculate_new_distribution(failed, 2, Dict, Diff) ->
    ok.





















