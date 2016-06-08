-ifndef(EMONGO_PUBLIC).

-record(response, {header, response_flag, cursor_id, offset, limit, documents}).

% Additional options that can be passed to emongo:find()
-define(TAILABLE_CURSOR, 2).
-define(SLAVE_OK, 4).
-define(USE_PRIMARY, use_primary).
-define(OPLOG, 8).
-define(NO_CURSOR_TIMEOUT, 16).
-define(AWAIT_DATA, 32).
-define(EXHAUST, 64).
-define(PARTIAL, 128).

-define(EMONGO_PUBLIC, true).
-endif.
