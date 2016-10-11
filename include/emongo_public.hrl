-ifndef(EMONGO_PUBLIC).

-record(response, {header, response_flag, cursor_id, offset, limit, documents}).

% Additional options that can be passed to emongo:find()
-define(TAILABLE_CURSOR, 2).
-define(OPLOG, 8).
-define(NO_CURSOR_TIMEOUT, 16).
-define(AWAIT_DATA, 32).
-define(EXHAUST, 64).
-define(PARTIAL, 128).

% Read Preferences (https://docs.mongodb.com/manual/core/read-preference/):
%   primary, primaryPreferred, secondary, secondaryPreferred, nearest
-define(SLAVE_OK,      {read_pref, <<"secondaryPreferred">>}). % For backwards compatibility
-define(USE_PRIMARY,   {read_pref, <<"primary">>}).
-define(USE_PRIM_PREF, {read_pref, <<"primaryPreferred">>}).
-define(USE_SECONDARY, {read_pref, <<"secondary">>}).
-define(USE_SECD_PREF, {read_pref, <<"secondaryPreferred">>}).
-define(USE_NEAREST,   {read_pref, <<"nearest">>}).

-define(EMONGO_PUBLIC, true).
-endif.
