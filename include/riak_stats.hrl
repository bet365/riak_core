%%%-----------------------------------------------------------------------------
%%% @doc APP specific @end
%%%-----------------------------------------------------------------------------

-define(APP,                  stats).
-define(PERSIST_APP,          riak_stats_metadata).
-define(PERSIST_ENV,          metadata_enabled).
%%                      attributes for all the metrics stored in exometer
-define(INFO,   [name, type, module, value, cache, status, timestamp, options]).

%%%-----------------------------------------------------------------------------
%%% @doc Stats @end
%%%-----------------------------------------------------------------------------

-type input_stats()         :: [tuple_stat()].
-type tuple_stat()          :: {stat_name(), type()}
                             | {stat_name(), type(), options()}
                             | {stat_name(), type(), options(), aliases()}.

-type stat_name()           :: [atom()] | list().
-type stats()               :: [stat_name()].
-export_type([stats/0]).

-type type()                :: exometer:type() | '_'.
-type options()             :: exometer:options().
-type aliases()             :: [alias()].
-type alias()               :: exometer_alias:alias().

-type stat_path()           :: [stat_name()] | [atom()].
-type found_entries()       :: {found_stats(), datapoints()}.
-type found_stats()         :: [{stat_name(),type(),status()}] | [].
-type status()              :: enabled | disabled | unregistered | '_'.
-type datapoints()          :: [atom()] | list() | [].

-type legacy_stat()         :: [{stat_name(),{alias(),list()}}].

-type found_values()        :: [{stat_name(), value() | values()}].
-type values()              :: [value()].
-type value()               :: {atom(), any()} | exometer:value().

%%%-----------------------------------------------------------------------------
%%% @doc Console @end
%%%-----------------------------------------------------------------------------

-type sanitised_stat()      :: {stat_name(),status(),type(),datapoints()}.

-type console_arg()         :: [string()] | list().
-type attributes()          :: [info()] | [].
-type info()                :: exometer:info().

%%%-----------------------------------------------------------------------------
%%% @doc Profile @end
%%%-----------------------------------------------------------------------------

-type profile_name()        :: console_arg().
