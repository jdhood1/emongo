-include_lib("emongo_public.hrl").

-define(CONN_TIMEOUT, 5000).
-define(OP_REPLY, 1).
-define(OP_MSG, 1000).
-define(OP_UPDATE, 2001).
-define(OP_INSERT, 2002).
-define(OP_QUERY, 2004).
-define(OP_GET_MORE, 2005).
-define(OP_DELETE, 2006).
-define(OP_KILL_CURSORS, 2007).
-define(SYS_NAMESPACES, "system.namespaces").

%-include_lib("eunit/include/eunit.hrl").
%-define(OUT(Fmt, Args), ?debugFmt(Fmt, Args)).
-define(OUT(Fmt, Args), io:format(Fmt, Args)).

-define(EXCEPTION(Fmt, Args, StackTrace),
  ?OUT("EXCEPTION (~p:~p): " Fmt "\n~p\n", [?MODULE, ?LINE | Args] ++ [StackTrace])).
-define(ERROR(Fmt, Args), ?OUT("ERROR (~p:~p): "     Fmt "\n",     [?MODULE, ?LINE | Args])).
-define(WARN(Fmt, Args),  ?OUT("WARNING (~p:~p): "   Fmt "\n",     [?MODULE, ?LINE | Args])).
-define(INFO(Fmt, Args),  ?OUT("INFO (~p:~p): "      Fmt "\n",     [?MODULE, ?LINE | Args])).
-define(DEBUG(Fmt, Args), ?OUT("DEBUG (~p:~p): "     Fmt "\n",     [?MODULE, ?LINE | Args])).
-define(DUMP(X),          ?DEBUG("~p = ~p", [??X, X])).

-define(IS_LIST_DOCUMENT(Doc),
  ( is_list(Doc) andalso
    ( Doc == [] orelse
      ( is_tuple(hd(Doc)) andalso
        tuple_size(hd(Doc)) == 2
      )
    )
  )
).
-define(IS_DOCUMENT(Doc),
  ?IS_LIST_DOCUMENT(Doc) or
  ( is_tuple(Doc) andalso
    tuple_size(Doc) == 2 andalso
    element(1, Doc) == struct andalso
    ?IS_LIST_DOCUMENT(element(2, Doc))
  )
).
-define(IS_LIST_OF_DOCUMENTS(Docs),
  ( is_list(Docs) andalso
	  ( Docs == [] orelse
      ?IS_DOCUMENT(hd(Docs))
	  )
	)
).

-record(pool, {
  id,
  host,
  port,
  database,
  size           = 1,
  auth_db        = undefined,
  user           = undefined,
  pass_hash      = undefined,
  socket_options = [],
  conns          = queue:new(),
  req_id         = 1
}).

-record(header, {message_length, request_id, response_to, op_code}).
-record(emo_query, {opts=0, offset=0, limit=0, q=[], field_selector=[]}).
