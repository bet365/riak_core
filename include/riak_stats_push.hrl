%%%-----------------------------------------------------------------------------
%%% @doc MACROS and type specific for the push functionality. @end
%%%-----------------------------------------------------------------------------
-define(PUSH_PREFIX, {stats_push, node()}).

-define(REFRESH_INTERVAL,       30000).
-define(SPIRAL_TIME_SPAN,       1000).
-define(HISTOGRAM_TIME_SPAN,    1000).
-define(STATS_LISTEN_PORT,      9000).
-define(STATS_UPDATE_INTERVAL,  1000).

-define(BUFFER,                 {buffer,    100*1024*1024}).
-define(SNDBUF,                 {sndbuf,      5*1024*1024}).
-define(ACTIVE,                 {active,    true}).
-define(REUSE,                  {reuseaddr, true}).

-define(OPTIONS,                [?BUFFER, ?SNDBUF, ?ACTIVE, ?REUSE]).

-type protocol()        :: tcp | udp | '_'.
-type instance()        :: string() | '_'.
-type socket()          :: inet:socket().
-type host()            :: inet:hostname() | inet:ip4_address() |undefined |'_'.
-type sanitised_push()  :: {{port()|'_',instance(),host()},stats:stats()|['_']}.

-type push_arg()        :: {push_key(),push_value()} | list().
-type push_key()        :: {protocol(),instance()}.
-type push_value()      ::  push_map() | atom().
-type push_map()        :: #{original_dt  := calendar:datetime(),
                             modified_dt  := calendar:datetime(),
                             pid          := pid() | undefined,
                             running      := true | false,
                             node         := node(),
                             port         := port() | undefined,
                             host         := host(),
                             stats        := stats:stats() | ['_']}.