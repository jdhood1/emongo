% Copyright (c) 2009 Jacob Vorreuter <jacob.vorreuter@gmail.com>
%
% Permission is hereby granted, free of charge, to any person
% obtaining a copy of this software and associated documentation
% files (the "Software"), to deal in the Software without
% restriction, including without limitation the rights to use,
% copy, modify, merge, publish, distribute, sublicense, and/or sell
% copies of the Software, and to permit persons to whom the
% Software is furnished to do so, subject to the following
% conditions:
%
% The above copyright notice and this permission notice shall be
% included in all copies or substantial portions of the Software.
%
% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
% EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
% OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
% NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
% HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
% WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
% FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
% OTHER DEALINGS IN THE SOFTWARE.
-module(emongo).
-behaviour(gen_server).
-include("emongo.hrl").

-export([start_link/0, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-export([oid/0, oid_generation_time/1,
         pools/0, add_pool/5, add_pool/6, remove_pool/1, update_pool_options/2,
         queue_lengths/0,
         register_collections_to_databases/2,
         find/2,     find/3,     find/4,
         find_all/2, find_all/3, find_all/4,
         find_one/2, find_one/3, find_one/4,
         kill_cursors/2,
         insert/3,
         insert_sync/3, insert_sync/4,
         update/4, update/5, update/6,
         update_all/4, update_all/5,
         update_sync/4, update_sync/5, update_sync/6,
         update_all_sync/4, update_all_sync/5,
         find_and_modify/4, find_and_modify/5,
         delete/2, delete/3, delete/4,
         delete_sync/2, delete_sync/3, delete_sync/4,
         distinct/3, distinct/4, distinct/5,
         ensure_index/4, create_index/4, create_index/5, drop_index/3,
         aggregate/3, aggregate/4,
         get_collections/1, get_collections/2,
         run_command/3, run_command/2,
         count/2, count/3, count/4,
         total_db_time_usec/0, db_timing/0, clear_timing/0,
         dec2hex/1, hex2dec/1, utf8_encode/1,
         drop_collection/2, drop_collection/3,
         drop_database/1, drop_database/2, get_databases/1, get_databases/2,
         strip_selector/1,
         cur_time_ms/0,
         get_config/2]).

-define(TIMING_KEY,      emongo_timing).
-define(MAX_TIMES,       10).
-define(SHA1_DIGEST_LEN, 20).

-record(state, {pools, oid_index, hashed_hostn}).

%====================================================================
% Types
%====================================================================
% pool_id() = atom()
% collection() = string()
% response() = {response, header, response_flag, cursor_id, offset, limit,
%               documents}
% documents() = [document()]
% document() = [{term(), term()}]

%====================================================================
% API
%====================================================================
%--------------------------------------------------------------------
% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
% Description: Starts the server
%--------------------------------------------------------------------
start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

pools() ->
  gen_server:call(?MODULE, pools, infinity).

oid() ->
  {OidIndex, HashedHN, Pid} = gen_server:call(?MODULE, oid_info, infinity),
  UnixTime = cur_time_ms(),
  <<_:20/binary, PidPart:2/binary, _/binary>> = term_to_binary(Pid),
  <<UnixTime:32/signed, HashedHN/binary, PidPart/binary, OidIndex:24>>.

oid_generation_time({oid, Oid}) ->
  oid_generation_time(Oid);
oid_generation_time(Oid) when is_binary(Oid) andalso size(Oid) =:= 12 ->
  <<UnixTime:32/signed, _/binary>> = Oid,
  UnixTime.

add_pool(PoolId, Host, Port, DefaultDatabase, Size) ->
  add_pool(PoolId, Host, Port, DefaultDatabase, Size, []).

% Options = [option()]
% option() =
%   {timeout,             integer()} % milliseconds
%   {auth_db,             string()}
%   {user,                string()}
%   {password,            string()}
%   % max_pipeline_depth is the maximum number of messages that are waiting for a response from the DB that can be sent
%   % across a socket before receiving a reply.  For example, if max_pipeline_depth is 1, only one message that requires
%   % a response will be sent across a given socket in the pool to the DB before a reply to that request is received.
%   % Other requests will wait in the queue.  Messages that do not require a response can still be sent on that socket.
%   % 0 means there is no limit.  This is equivalent to HTTP pipelining, except in the communication with the DB.
%   {max_pipeline_depth,  int()}
%   {socket_options,      [gen_tcp:connect_option()]} % http://www.erlang.org/doc/man/gen_tcp.html#type-connect_option
%   {write_concern,       int()}
%   {write_concern_timeout, integer()} % milliseconds
%   {journal_write_ack,   boolean()}
%   % disconnect_timeouts is the number of consecutive timeouts allowed on a socket before the socket is disconnected
%   % and reconnect.  0 means even the first timeout will cause a disconnect and reconnect.
%   {disconnect_timeouts, int()}
%   {default_read_pref,   string()} % https://docs.mongodb.com/manual/core/read-preference/
%      primary, primaryPreferred, secondary, secondaryPreferred, nearest
%   % max_batch_size is the max number of documents that can be read from the DB at a time.  0 means MongoDB determines
%   % the limit.
%   {max_batch_size,          int()}

add_pool(PoolId, Host, Port, DefaultDatabase, Size, Options) ->
  update_pool_options(PoolId, [
    {default_read_pref, to_binary(proplists:get_value(default_read_pref,     Options, undefined))},
    {max_batch_size,              proplists:get_value(max_batch_size,        Options, 0)},
    {timeout,                     proplists:get_value(timeout,               Options, 5000)},
    {max_pipeline_depth,          proplists:get_value(max_pipeline_depth,    Options, 0)},
    {write_concern,               proplists:get_value(write_concern,         Options, 1)},
    {write_concern_timeout,       proplists:get_value(write_concern_timeout, Options, 4000)},
    {journal_write_ack,           proplists:get_value(journal_write_ack,     Options, false)},
    {disconnect_timeouts,         proplists:get_value(disconnect_timeouts,   Options, 10)}
  ]),
  gen_server:call(?MODULE, {add_pool, create_pool(PoolId, Host, Port, DefaultDatabase, Size, Options)}, infinity).

create_pool(PoolId, Host, Port, DefaultDatabase, Size, Options) ->
  Def = #pool{},
  AuthDatabase        = proplists:get_value(auth_db,        Options, undefined),
  User      = to_binary(proplists:get_value(user,           Options, Def#pool.user)),
  Password  = to_binary(proplists:get_value(password,       Options, undefined)),
  SocketOptions       = proplists:get_value(socket_options, Options, Def#pool.socket_options),
  #pool{
    id             = PoolId,
    host           = to_list(Host),
    port           = Port,
    database       = DefaultDatabase,
    size           = Size,
    auth_db        = AuthDatabase,
    user           = User,
    pass_hash      = pass_hash(User, Password),
    socket_options = SocketOptions
  }.

remove_pool(PoolId) ->
  gen_server:call(?MODULE, {remove_pool, PoolId}).

% The following pool options can be updated dynamically:
-define(DYNAMIC_POOL_OPTIONS, [
  default_read_pref,
  max_batch_size,
  timeout,
  max_pipeline_depth,
  write_concern,
  write_concern_timeout,
  journal_write_ack,
  disconnect_timeouts
]).
% For information about the input Options, see add_pool(...).
update_pool_options(PoolId, Options) ->
  lists:foreach(fun({Name, Value}) ->
    case lists:member(Name, ?DYNAMIC_POOL_OPTIONS) of
      true -> save_config(PoolId, Name, Value);
      _    -> ?WARN("Ignoring invalid option: ~p", [{Name, Value}])
    end
  end, Options).

queue_lengths() ->
  lists:map(fun({PoolId, #pool{conns = Queue}}) ->
    Conns = queue:to_list(Queue),
    QueueLens = [emongo_conn:queue_lengths(Conn) || Conn <- Conns],
    {PoolId, lists:sum(QueueLens)}
  end, pools()).

% The default database is passed in when a pool is created.  However, if you want to use that same pool to talk to other
% databases, you can override the default database on a per-collection basis.  To do that, call this function.
% Input is either in the format [{Collection, Database}] or [{Collection, Database, AuthFlag}], where the AuthFlag is a
% boolean that specifies whether to authenticate with the database.
% If no AuthFlag is specified, the databases is authenticated.
register_collections_to_databases(PoolId, CollDbMap) ->
  lists:map(fun(CollDb) ->
    {Collection, Database, AuthFlag} = case CollDb of
      {Coll, Db}     -> {Coll, Db, true};
      {Coll, Db, AF} -> {Coll, Db, AF}
    end,
    save_config(PoolId, {coll_to_db, to_binary(Collection)}, {Database, AuthFlag})
  end, CollDbMap),
  gen_server:call(?MODULE, {authorize_new_dbs, PoolId}, 60000).

%------------------------------------------------------------------------------
% find
%------------------------------------------------------------------------------
% @spec find(PoolId, Collection, Selector, Options) -> Result
%     PoolId = atom()
%     Collection = string()
%     Selector = document()
%     Options = [Option]
%     Option = {timeout, Timeout} | {limit, Limit} | {offset, Offset} |
%              {orderby, Orderby} | {fields, Fields} | response_options
%     Timeout = integer (timeout in milliseconds)
%     Limit = integer >= 0
%     Offset = integer
%     Orderby = [{Key, Direction}]
%     Key = string() | binary() | atom() | integer()
%     Direction = asc | desc
%     Fields = [Field]
%     Field = string() | binary() | atom() | integer() = specifies a field to
%             return in the result set
%     response_options = return {response, header, response_flag, cursor_id,
%                                offset, limit, documents}
%     Result = documents() | response()
find(PoolId, Collection)           -> find(PoolId, Collection, [],       []).
find(PoolId, Collection, Selector) -> find(PoolId, Collection, Selector, []).
find(PoolId, Collection, Selector, Options) when ?IS_DOCUMENT(Selector), is_list(Options) ->
  BatchSize       = batch_size(PoolId, Options),
  Timeout         = get_timeout(PoolId, Options),
  ReadPrefOptions = set_read_preference(PoolId, Options),
  LimitOptions    = [{limit, BatchSize} | lists:keydelete(limit, 1, ReadPrefOptions)],
  FindOptions     = [{<<"$maxTimeMS">>, Timeout} | LimitOptions],
  Query           = create_query(FindOptions, Selector),
  {Conn, Pool}    = gen_server:call(?MODULE, {conn, PoolId}, infinity),
  Packet          = emongo_packet:do_query(get_database(Pool, Collection), Collection, Pool#pool.req_id, Query),
  RespPre         = send_recv_command(find, Collection, Selector, FindOptions, Conn, Pool, Packet),
  Resp            = get_all(PoolId, Collection, Options, RespPre),
  case lists:member(response_options, Options) of
    true  -> Resp;
    false -> response_docs(Resp)
  end.

batch_size(PoolId, Options) -> batch_size(PoolId, Options, 0).
batch_size(PoolId, Options, NumDocs) when is_list(Options) ->
  batch_size(PoolId, proplists:get_value(limit, Options, 0), NumDocs);
batch_size(PoolId, 0, _) -> get_config(PoolId, max_batch_size);
batch_size(PoolId, Limit, NumDocs) when Limit > NumDocs ->
  NumRemaining = Limit - NumDocs,
  MaxBatchSize = get_config(PoolId, max_batch_size),
  case (MaxBatchSize /= 0) and (NumRemaining >= MaxBatchSize) of
    true -> MaxBatchSize;
    _    ->
      % We could make this number negative to tell MongoDB to delete the cursor when this call is done.  However, with
      % aggregate calls, the batch size has to be non-negative.
      NumRemaining
  end;
batch_size(_PoolId, Limit, _) when Limit < 0 -> throw({emongo_error, {incorrect_limit, Limit}});
batch_size(_PoolId, _, _) -> limit_reached.

get_all(PoolId, Collection, Options, Resp) ->
  NumDocs   = length(response_docs(Resp)),
  BatchSize = batch_size(PoolId, Options, NumDocs),
  get_all(PoolId, Collection, BatchSize, Options, Resp).

get_all(_PoolId, _Collection,  limit_reached, _Options, Resp)                            -> Resp;
get_all(_PoolId, _Collection, _BatchSize,     _Options, #response{cursor_id = 0} = Resp) -> Resp;
get_all( PoolId,  Collection,  BatchSize,      Options, #response{cursor_id = CurId} = OldResp) ->
  {Conn, Pool} = gen_server:call(?MODULE, {conn, PoolId}, infinity),
  Packet       = emongo_packet:get_more(get_database(Pool, Collection), Collection, Pool#pool.req_id, BatchSize, CurId),
  NewResp      = send_recv_command(get_all, Collection, CurId, [{batch_size, BatchSize} | Options], Conn, Pool, Packet),
  DocsSoFar    = lists:append(response_docs(OldResp), response_docs(NewResp)),
  get_all(PoolId, Collection, Options, NewResp#response{documents = DocsSoFar}).

%------------------------------------------------------------------------------
% find_all
% If the 'limit' option is used, it determines the batch size, not the number of documents returned.  All documents will
% be returned, even if that number exceeds the limit.  Use find(...) to limit the number of documents returned.
%------------------------------------------------------------------------------
find_all(PoolId, Collection)                    -> find(PoolId, Collection).
find_all(PoolId, Collection, Selector)          -> find(PoolId, Collection, Selector).
find_all(PoolId, Collection, Selector, Options) -> find(PoolId, Collection, Selector, Options).

%------------------------------------------------------------------------------
% find_one
%------------------------------------------------------------------------------
find_one(PoolId, Collection)           -> find_one(PoolId, Collection, [],       []).
find_one(PoolId, Collection, Selector) -> find_one(PoolId, Collection, Selector, []).
find_one(PoolId, Collection, Selector, Options) when is_list(Options) ->
  find(PoolId, Collection, Selector, [{limit, 1} | lists:keydelete(limit, 1, Options)]).

%------------------------------------------------------------------------------
% kill_cursors
%------------------------------------------------------------------------------
kill_cursors(PoolId, CursorID) when is_integer(CursorID) ->
  kill_cursors(PoolId, [CursorID]);

kill_cursors(PoolId, CursorIDs) when is_list(CursorIDs) ->
  {Conn, Pool} = gen_server:call(?MODULE, {conn, PoolId}, infinity),
  Packet = emongo_packet:kill_cursors(Pool#pool.req_id, CursorIDs),
  send_command(kill_cursors, undefined, CursorIDs, [], Conn, Pool, Packet).

%------------------------------------------------------------------------------
% insert
%------------------------------------------------------------------------------
insert(PoolId, Collection, DocumentsIn) ->
  Documents = if
    ?IS_LIST_OF_DOCUMENTS(DocumentsIn) -> DocumentsIn;
    ?IS_DOCUMENT(DocumentsIn)          -> [DocumentsIn]
  end,
  {Conn, Pool} = gen_server:call(?MODULE, {conn, PoolId}, infinity),
  Packet = emongo_packet:insert(get_database(Pool, Collection), Collection,
                                Pool#pool.req_id, Documents),
  send_command(insert, Collection, undefined, [], Conn, Pool, Packet).

%------------------------------------------------------------------------------
% insert_sync that runs db.$cmd.findOne({getlasterror: 1});
% Options can include:
%   {write_concern, string() | integer()}
%   {write_concern_timeout, integer()} (timeout in milliseconds)
%   {journal_write_ack, boolean()} (version 2.6 or later)
%   {ordered, boolean()} (version 2.6 or later)
%------------------------------------------------------------------------------
insert_sync(PoolId, Collection, Documents) ->
  insert_sync(PoolId, Collection, Documents, []).

insert_sync(_PoolId, _Collection, [], _Options) ->
  % Note that, if response_options is passed in with an empty list of documents to be inserted, 'ok' is still returned
  % instead of a #response{} record.
  ok;
insert_sync(PoolId, Collection, DocumentsIn, Options) ->
  Documents = if
    ?IS_LIST_OF_DOCUMENTS(DocumentsIn) -> DocumentsIn;
    ?IS_DOCUMENT(DocumentsIn)          -> [DocumentsIn]
  end,
  {FirstPage, Rest} = next_bulk_page(PoolId, Documents),
  Resp = insert_sync_page(PoolId, Collection, FirstPage, Rest, Options, undefined),
  get_sync_result(Resp, Options).

insert_sync_page(_, _, [], _, _, AccResp) ->
  AccResp;
insert_sync_page(PoolId, Collection, FirstPage, Rest, Options, AccResp) ->
  Ordered      = proplists:get_value(ordered, Options, true),
  InsertDoc    = [{<<"documents">>, {array, FirstPage}},
                  get_ordered_option(Ordered),
                  get_writeconcern_option(PoolId, Options)],
  Query        = create_cmd(<<"insert">>, Collection, InsertDoc, -1, Options),
  {Conn, Pool} = gen_server:call(?MODULE, {conn, PoolId}, infinity),
  Packet       = emongo_packet:do_query(get_database(Pool, Collection), "$cmd", Pool#pool.req_id, Query),
  Resp         = send_recv_command(insert_sync, Collection, undefined, Options, Conn, Pool, Packet),
  NewAccResp   = merge_insert_sync_page_response(AccResp, Resp),

  % Break out early if the request is ordered and a write error occurred
  case Ordered andalso proplists:is_defined(<<"writeErrors">>, response_first_doc(Resp)) of
    true ->
      NewAccResp;
    false ->
      {NextPage, NewRest} = next_bulk_page(PoolId, Rest),
      insert_sync_page(PoolId, Collection, NextPage, NewRest, Options, NewAccResp)
  end.

%------------------------------------------------------------------------------
% update
%------------------------------------------------------------------------------
update(PoolId, Collection, Selector, Document) when ?IS_DOCUMENT(Selector),
                                                    ?IS_DOCUMENT(Document) ->
  update(PoolId, Collection, Selector, Document, false).

update(PoolId, Collection, Selector, Document, Upsert) ->
  update(PoolId, Collection, Selector, Document, Upsert, []).

update(PoolId, Collection, Selector, Document, Upsert, Options)
    when ?IS_DOCUMENT(Selector), ?IS_DOCUMENT(Document) ->
  Query = create_query(Options, Selector),
  {Conn, Pool} = gen_server:call(?MODULE, {conn, PoolId}, infinity),
  Packet = emongo_packet:update(get_database(Pool, Collection), Collection,
                                Pool#pool.req_id, Upsert, false, Query#emo_query.q,
                                Document),
  send_command(update, Collection, Selector, [{upsert, Upsert}], Conn, Pool, Packet).

%------------------------------------------------------------------------------
% update_all
%------------------------------------------------------------------------------
update_all(PoolId, Collection, Selector, Document) ->
  update_all(PoolId, Collection, Selector, Document, []).

update_all(PoolId, Collection, Selector, Document, Options)
    when ?IS_DOCUMENT(Selector), ?IS_DOCUMENT(Document) ->
  Query = create_query(Options, Selector),
  {Conn, Pool} = gen_server:call(?MODULE, {conn, PoolId}, infinity),
  Packet = emongo_packet:update(get_database(Pool, Collection), Collection,
                                Pool#pool.req_id, false, true, Query#emo_query.q,
                                Document),
  send_command(update_all, Collection, Selector, [], Conn, Pool, Packet).

%------------------------------------------------------------------------------
% update_sync that runs db.$cmd.findOne({getlasterror: 1});
% Options can include:
%   {write_concern, string() | integer()}
%   {write_concern_timeout, integer()} (timeout in milliseconds)
%   {journal_write_ack, boolean()} (version 2.6 or later)
%   {ordered, boolean()} (version 2.6 or later)
%------------------------------------------------------------------------------
update_sync(PoolId, Collection, Selector, Document)
    when ?IS_DOCUMENT(Selector), ?IS_DOCUMENT(Document) ->
  update_sync(PoolId, Collection, Selector, Document, false).

update_sync(PoolId, Collection, Selector, Document, Upsert)
    when ?IS_DOCUMENT(Selector), ?IS_DOCUMENT(Document) ->
  update_sync(PoolId, Collection, Selector, Document, Upsert, []).

update_sync(PoolId, Collection, Selector, Document, Upsert, Options)
    when ?IS_DOCUMENT(Selector), ?IS_DOCUMENT(Document) ->
  UpdateDoc = [{<<"updates">>, [[{<<"q">>,      {struct, transform_selector(Selector)}},
                                 {<<"u">>,      {struct, Document}},
                                 {<<"upsert">>, Upsert}]]},
               get_ordered_option(Options),
               get_writeconcern_option(PoolId, Options)],
  Query   = create_cmd(<<"update">>, Collection, UpdateDoc, -1, Options),
  {Conn, Pool} = gen_server:call(?MODULE, {conn, PoolId}, infinity),
  Packet  = emongo_packet:do_query(get_database(Pool, Collection), "$cmd", Pool#pool.req_id, Query),
  Resp    = send_recv_command(update_sync, Collection, Selector, Options, Conn, Pool, Packet),
  case get_sync_result(Resp, Options) of
    ok  -> response_n(Resp);
    Ret -> Ret
  end.

%------------------------------------------------------------------------------
% update_all_sync that runs db.$cmd.findOne({getlasterror: 1});
% Options can include:
%   {write_concern, string() | integer()}
%   {write_concern_timeout, integer()} (timeout in milliseconds)
%   {journal_write_ack, boolean()} (version 2.6 or later)
%   {ordered, boolean()} (version 2.6 or later)
%------------------------------------------------------------------------------
update_all_sync(PoolId, Collection, Selector, Document)
    when ?IS_DOCUMENT(Selector), ?IS_DOCUMENT(Document) ->
  update_all_sync(PoolId, Collection, Selector, Document, []).

update_all_sync(PoolId, Collection, Selector, Document, Options)
    when ?IS_DOCUMENT(Selector), ?IS_DOCUMENT(Document) ->
  UpdateDoc = [{<<"updates">>, [[{<<"q">>,     {struct, transform_selector(Selector)}},
                                 {<<"u">>,     {struct, Document}},
                                 {<<"multi">>, true}]]},
               get_ordered_option(Options),
               get_writeconcern_option(PoolId, Options)],
  Query   = create_cmd(<<"update">>, Collection, UpdateDoc, -1, Options),
  {Conn, Pool} = gen_server:call(?MODULE, {conn, PoolId}, infinity),
  Packet  = emongo_packet:do_query(get_database(Pool, Collection), "$cmd", Pool#pool.req_id, Query),
  Resp    = send_recv_command(update_all_sync, Collection, Selector, Options, Conn, Pool, Packet),
  case get_sync_result(Resp, Options) of
    ok  -> response_n(Resp);
    Ret -> Ret
  end.

%------------------------------------------------------------------------------
% delete
%------------------------------------------------------------------------------
delete(PoolId, Collection) ->
  delete(PoolId, Collection, []).

delete(PoolId, Collection, Selector) ->
  delete(PoolId, Collection, Selector, []).

delete(PoolId, Collection, Selector, Options) ->
  Query = create_query(Options, Selector),
  {Conn, Pool} = gen_server:call(?MODULE, {conn, PoolId}, infinity),
  Packet = emongo_packet:delete(get_database(Pool, Collection), Collection,
                                Pool#pool.req_id, Query#emo_query.q),
  send_command(delete, Collection, Selector, Options, Conn, Pool, Packet).

%------------------------------------------------------------------------------
% delete_sync that runs db.$cmd.findOne({getlasterror: 1});
% Options can include:
%   {write_concern, string() | integer()}
%   {write_concern_timeout, integer()} (timeout in milliseconds)
%   {journal_write_ack, boolean()} (version 2.6 or later)
%   {ordered, boolean()} (version 2.6 or later)
%------------------------------------------------------------------------------
delete_sync(PoolId, Collection) ->
  delete_sync(PoolId, Collection, []).

delete_sync(PoolId, Collection, Selector) ->
  delete_sync(PoolId, Collection, Selector, []).

delete_sync(PoolId, Collection, Selector, Options) ->
  DeleteDoc = [{<<"deletes">>, [[{<<"q">>,     {struct, transform_selector(Selector)}},
                                 {<<"limit">>, 0}]]},
               get_ordered_option(Options),
               get_writeconcern_option(PoolId, Options)],
  Query   = create_cmd(<<"delete">>, Collection, DeleteDoc, -1, Options),
  {Conn, Pool} = gen_server:call(?MODULE, {conn, PoolId}, infinity),
  Packet  = emongo_packet:do_query(get_database(Pool, Collection), "$cmd", Pool#pool.req_id, Query),
  Resp    = send_recv_command(delete_sync, Collection, Selector, Options, Conn, Pool, Packet),
  case get_sync_result(Resp, Options) of
    ok  -> response_n(Resp);
    Ret -> Ret
  end.

%------------------------------------------------------------------------------
% ensure index (deprecated, use create_index instead)
%------------------------------------------------------------------------------
ensure_index(PoolId, Collection, Keys, Unique) ->
  Options = case Unique of
    true -> [{<<"unique">>, true}];
    _    -> []
  end,
  create_index(PoolId, Collection, Keys, undefined, Options).

%------------------------------------------------------------------------------
% create index
%------------------------------------------------------------------------------
create_index(PoolId, Collection, Keys, Options) when ?IS_DOCUMENT(Keys)  ->
  create_index(PoolId, Collection, Keys, undefined, Options).

create_index(PoolId, Collection, Keys, IndexName, Options) when ?IS_DOCUMENT(Keys) ->
  Name = case IndexName of
    undefined -> generate_index_name(Keys, <<>>);
    _         -> IndexName
  end,
  Index        = [{<<"key">>, Keys}, {<<"name">>, Name}] ++ Options,
  CreateIdxDoc = [{<<"indexes">>, {array, [Index]}}],
  Query        = create_cmd(<<"createIndexes">>, Collection, CreateIdxDoc, -1, []),
  {Conn, Pool} = gen_server:call(?MODULE, {conn, PoolId}, infinity),
  Packet       = emongo_packet:do_query(get_database(Pool, Collection), "$cmd", Pool#pool.req_id, Query),
  Resp         = send_recv_command(create_index, Collection, undefined, [], Conn, Pool, Packet),
  get_sync_result(Resp, Options).

generate_index_name([], AccName) ->
  AccName;
generate_index_name([{Key, Value} | Rest], <<>>) ->
  ValueBin = integer_to_binary(Value),
  generate_index_name(Rest, << Key/binary, "_", ValueBin/binary >>);
generate_index_name([{Key, Value} | Rest], AccName) ->
  ValueBin = integer_to_binary(Value),
  generate_index_name(Rest, << AccName/binary, "_", Key/binary, "_", ValueBin/binary >>).

%------------------------------------------------------------------------------
% drop index
%------------------------------------------------------------------------------
drop_index(PoolId, Collection, IndexName) ->
  DropIdxDoc   = [{<<"index">>, IndexName}],
  Query        = create_cmd(<<"dropIndexes">>, Collection, DropIdxDoc, -1, []),
  {Conn, Pool} = gen_server:call(?MODULE, {conn, PoolId}, infinity),
  Packet       = emongo_packet:do_query(get_database(Pool, Collection), "$cmd", Pool#pool.req_id, Query),
  Resp         = send_recv_command(drop_index, Collection, undefined, [], Conn, Pool, Packet),
  get_sync_result(Resp, []).

%------------------------------------------------------------------------------
% count
%------------------------------------------------------------------------------
count(PoolId, Collection) ->
  count(PoolId, Collection, [], []).

count(PoolId, Collection, Selector) ->
  count(PoolId, Collection, Selector, []).

count(PoolId, Collection, Selector, OptionsIn) ->
  Options      = set_read_preference(PoolId, OptionsIn),
  CountDoc     = [{<<"query">>, {struct, transform_selector(Selector)}}],
  Query        = create_cmd(<<"count">>, Collection, CountDoc, 1, Options),
  {Conn, Pool} = gen_server:call(?MODULE, {conn, PoolId}, infinity),
  Packet       = emongo_packet:do_query(get_database(Pool, Collection), "$cmd", Pool#pool.req_id, Query),
  Resp         = send_recv_command(count, Collection, Selector, Options, Conn, Pool, Packet),
  case lists:member(response_options, Options) of
    true  -> Resp;
    false -> response_n(Resp)
  end.

%%------------------------------------------------------------------------------
%% aggregate
%%------------------------------------------------------------------------------
aggregate(PoolId, Collection, Pipeline) ->
  aggregate(PoolId, Collection, Pipeline, []).

aggregate(PoolId, Collection, Pipeline, OptionsIn) ->
  % As of MongoDB 3.6.x, the "cursor" option must be sent with all aggregate calls (except ones doing an "explain").
  % This means that the result is not a simple list of documents.  Rather, it is a "cursor" result, which includes the
  % "firstBatch" of results.  We then have to check the cursor and get the rest of the documents, if there are any.
  % When we get the rest of the documents, they are returned as normal documents, much like a "find" would return.
  % The results from these 2 different formats have to be merged together here and returned to the caller.
  OptionsPre    = set_read_preference(PoolId, OptionsIn),
  BatchSize     = batch_size(PoolId, OptionsPre),
  Options       = [{<<"cursor">>, [{<<"batchSize">>, BatchSize}]} | OptionsPre],
  AggregateDoc  = [{<<"pipeline">>, {array, Pipeline}}],
  Query         = create_cmd(<<"aggregate">>, Collection, AggregateDoc, 1, Options),
  {Conn, Pool}  = gen_server:call(?MODULE, {conn, PoolId}, infinity),
  Packet        = emongo_packet:do_query(get_database(Pool, Collection), "$cmd", Pool#pool.req_id, Query),
  Resp          = send_recv_command(aggregate, Collection, Pipeline, Options, Conn, Pool, Packet),
  RespOpts      = lists:member(response_options, Options),
  RespOk        = response_ok(Resp),
  if
    RespOpts -> Resp;
    RespOk ->
      CursorInfo    = proplists:get_value(<<"cursor">>,     response_first_doc(Resp)),
      {array, Docs} = proplists:get_value(<<"firstBatch">>, CursorInfo),
      CursorId      = proplists:get_value(<<"id">>,         CursorInfo, Resp#response.cursor_id),
      NewResp       = get_all(PoolId, Collection, Options, Resp#response{cursor_id = CursorId, documents = Docs}),
      response_docs(NewResp);
    true -> throw({emongo_aggregation_failed, Resp})
  end.

%%------------------------------------------------------------------------------
%% find_and_modify
% Options can include:
%   {timeout, integer()} (timeout in milliseconds)
%   {write_concern, string() | integer()} (version 3.2 or later)
%   {write_concern_timeout, integer()} (timeout in milliseconds) (version 3.2 or later)
%   {journal_write_ack, boolean()} (version 3.2 or later)
%%------------------------------------------------------------------------------
find_and_modify(PoolId, Collection, Selector, Update) ->
  find_and_modify(PoolId, Collection, Selector, Update, []).

find_and_modify(PoolId, Collection, Selector, Update, Options)
  when ?IS_DOCUMENT(Selector), ?IS_DOCUMENT(Update), is_list(Options) ->
  Timeout      = get_timeout(PoolId, Options),
  FindModDoc   = [{<<"query">>, {struct, transform_selector(Selector)}},
                  {<<"update">>, Update},
                  {<<"maxTimeMS">>, Timeout}],
  WriteConcern = [get_writeconcern_option(PoolId, Options)],
  Query        = create_cmd(<<"findandmodify">>, Collection, FindModDoc ++ WriteConcern, undefined,
                            % We don't want to force the limit to 1, but want to default it to 1 if it's not in Options.
                            [{limit, 1} | Options]),
  {Conn, Pool} = gen_server:call(?MODULE, {conn, PoolId}, infinity),
  Packet       = emongo_packet:do_query(get_database(Pool, Collection), "$cmd", Pool#pool.req_id, Query),
  Resp         = send_recv_command(find_and_modify, Collection, Selector, Options, Conn, Pool, Packet),
  case get_sync_result(Resp, Options) of
    ok  -> response_docs(Resp);
    Ret -> Ret
  end.

%====================================================================
% db collection operations
%====================================================================
drop_collection(PoolId, Collection) -> drop_collection(PoolId, Collection, []).

drop_collection(PoolId, Collection, Options) when is_atom(PoolId) ->
  TQuery       = create_query([], [{<<"drop">>, Collection}]),
  Query        = TQuery#emo_query{limit=-1}, %dont ask me why, it just has to be -1
  {Conn, Pool} = gen_server:call(?MODULE, {conn, PoolId}, infinity),
  Packet       = emongo_packet:do_query(get_database(Pool, Collection), "$cmd", Pool#pool.req_id, Query),
  Resp         = send_recv_command(drop_collection, "$cmd", Query, Options, Conn, Pool, Packet),
  RespOpts     = lists:member(response_options, Options),
  RespOk       = response_ok(Resp),
  if
    RespOpts -> Resp;
    RespOk   -> ok;
    true     -> throw({emongo_drop_collection_failed, error_msg(Resp)})
  end.

get_collections(PoolId) -> get_collections(PoolId, []).
get_collections(PoolId, OptionsIn) ->
  Options      = set_read_preference(PoolId, OptionsIn),
  Query        = create_query(Options, []),
  {Conn, Pool} = gen_server:call(?MODULE, {conn, PoolId}, infinity),
  Database     = to_binary(get_database(Pool, undefined)),
  Packet       = emongo_packet:do_query(Database, ?SYS_NAMESPACES, Pool#pool.req_id, Query),
  Resp         = send_recv_command(get_collections, ?SYS_NAMESPACES, Query, Options, Conn, Pool, Packet),
  case lists:member(response_options, Options) of
    true  -> Resp;
    false ->
      Docs = response_docs(Resp),
      DatabaseForSplit = <<Database/binary, ".">>,
      lists:foldl(fun(Doc, Accum) ->
        Collection = proplists:get_value(<<"name">>, Doc),
        case binary:match(Collection, <<".$">>) of
          nomatch ->
            [_Junk, RealName] = binary:split(Collection, DatabaseForSplit),
            [ RealName | Accum ];
          _ -> Accum
        end
      end, [], Docs)
  end.

get_databases(PoolId) -> get_databases(PoolId, []).

get_databases(PoolId, OptionsIn) ->
  Options      = set_read_preference(PoolId, OptionsIn),
  TQuery       = create_query(Options, [{<<"listDatabases">>, 1}]),
  Query        = TQuery#emo_query{limit=-1}, %dont ask me why, it just has to be -1
  Database     = <<"admin">>,
  {Conn, Pool} = gen_server:call(?MODULE, {conn, PoolId}, infinity),
  Packet       = emongo_packet:do_query(Database, "$cmd", Pool#pool.req_id, Query),
  Resp         = send_recv_command(get_databases, "$cmd", Query, Options, Conn, Pool, Packet),
  RespOpts     = lists:member(response_options, Options),
  RespOk       = response_ok(Resp),
  % {response, {header, 1234, 12345678, 123456, 1}, 1, 1, 1, 1,
  %   [[
  %     {<<"databases">>, {array, [
  %       [ {<<"name">>,       <<"DB1">>},
  %         {<<"sizeOnDisk">>, 12345678.0},
  %         {<<"empty">>,      false}],
  %       [ {<<"name">>,       <<"DB2">>},
  %         {<<"sizeOnDisk">>, 12345678.0},
  %         {<<"empty">>,      false}],
  %       ...
  %     ]}},
  %     {<<"totalSize">>, 1.2345678e9},
  %     {<<"ok">>,        1.0}
  %   ]]
  % }
  if
    RespOpts -> Resp;
    RespOk   ->
      Doc = response_first_doc(Resp),
      {array, Databases} = proplists:get_value(<<"databases">>, Doc),
      [proplists:get_value(<<"name">>, Db) || Db <- Databases];
    true     -> throw({emongo_get_databases_failed, error_msg(Resp)})
  end.

distinct(PoolId, Collection, Key) -> distinct(PoolId, Collection, Key, [], []).
distinct(PoolId, Collection, Key, Options) -> distinct(PoolId, Collection, Key, [], Options).
distinct(PoolId, Collection, Key, SubQuery, Options) ->
  Query0 = [
    { <<"distinct">>, Collection },
    { <<"key">>, Key }
  ],

  Query = case SubQuery of
    [] -> Query0;
    _  -> Query0 ++ [{<<"query">>, {struct, transform_selector(SubQuery)}}]
  end,

  case run_command(PoolId, Query, Options) of
    [PL] when is_list(PL) ->
      Values = proplists:get_value(<<"values">>, PL),
      case Values of
        { 'array', L } when is_list(L) -> L;
        _ -> throw({emongo_get_distinct_failed, PL})
      end;
    V -> throw({emongo_get_distinct_failed, V})
  end.

run_command(PoolId, Command) -> run_command(PoolId, Command, []).
run_command(PoolId, Command, Options) ->
  Query = #emo_query{q = Command, limit=-1},
  {Conn, Pool} = gen_server:call(?MODULE, {conn, PoolId}, infinity),
  Packet = emongo_packet:do_query(get_database(Pool, undefined), "$cmd", Pool#pool.req_id, Query),
  Resp = send_recv_command(command, "$cmd", Command, Options, Conn, Pool, Packet),
  case lists:member(response_options, Options) of
    true  -> Resp;
    false -> response_docs(Resp)
  end.

total_db_time_usec() ->
  case erlang:get(?TIMING_KEY) of
    undefined -> 0;
    Timing    -> lists:sum([proplists:get_value(time_usec, X, 0) || X <- Timing])
  end.

db_timing() ->
  erlang:get(?TIMING_KEY).

clear_timing() ->
  erlang:erase(?TIMING_KEY).

%====================================================================
% db holy cow operations
%====================================================================
%this is slightly ghetto fabulous, it just provides a very quick method to
%truncate the entire db and trash all of the collections
drop_database(PoolId) -> drop_database(PoolId, []).
drop_database(PoolId, Options) ->
    %doing this with emongo_conn:send_recv that way we do not get the last error from the
    %dropped db
  Selector = [{<<"dropDatabase">>, 1}],
  TQuery   = create_query([], Selector),
  Query    = TQuery#emo_query{limit=-1}, %dont ask me why, it just has to be -1
  {Conn, Pool} = gen_server:call(?MODULE, {conn, PoolId}, infinity),
  Packet   = emongo_packet:do_query(get_database(Pool, undefined), "$cmd", Pool#pool.req_id, Query),
  Resp     = send_recv_command(drop_database, undefined, Selector, Options, Conn, Pool, Packet),
  RespOpts = lists:member(response_options, Options),
  RespOk   = response_ok(Resp),
  if
    RespOpts -> Resp;
    RespOk   -> ok;
    true     -> throw({emongo_drop_database_failed, error_msg(Resp)})
  end.

%====================================================================
% gen_server callbacks
%====================================================================

%--------------------------------------------------------------------
% Function: init(Args) -> {ok, State} |
%             {ok, State, Timeout} |
%             ignore         |
%             {stop, Reason}
% Description: Initiates the server
%--------------------------------------------------------------------
init(_) ->
  process_flag(trap_exit, true),
  init_config(),
  Pools = initialize_pools(),
  {ok, HN} = inet:gethostname(),
  <<HashedHN:3/binary,_/binary>> = erlang:md5(HN),
  {ok, #state{pools=Pools, oid_index=1, hashed_hostn=HashedHN}}.

%--------------------------------------------------------------------
% Function: % handle_call(Request, From, State) -> {reply, Reply, State} |
%                    {reply, Reply, State, Timeout} |
%                    {noreply, State} |
%                    {noreply, State, Timeout} |
%                    {stop, Reason, Reply, State} |
%                    {stop, Reason, State}
% Description: Handling call messages
%--------------------------------------------------------------------
handle_call(pools, _From, State) ->
  {reply, State#state.pools, State};

handle_call(oid_info, _From, #state{oid_index = OidIndex, hashed_hostn = HashedHN} = State) ->
  {reply, {OidIndex rem 16#ffffff, HashedHN, self()}, State#state{oid_index = OidIndex + 1}};

handle_call({add_pool, NewPool = #pool{id = PoolId}}, _From, #state{pools = Pools} = State) ->
  {Result, Pools1} =
    case proplists:is_defined(PoolId, Pools) of
      true ->
        Pool = proplists:get_value(PoolId, Pools),
        Pool1 = do_open_connections(Pool),
        {ok, [{PoolId, Pool1} | proplists:delete(PoolId, Pools)]};
      false ->
        Pool1 = do_open_connections(NewPool),
        {ok, [{PoolId, Pool1} | Pools]}
    end,
  {reply, Result, State#state{pools=Pools1}};

handle_call({remove_pool, PoolId}, _From, #state{pools=Pools}=State) ->
  {Result, Pools1} =
    case proplists:get_value(PoolId, Pools, undefined) of
      undefined ->
        {not_found, Pools};
      #pool{conns = Conns} ->
        lists:foreach(fun(Conn) ->
          emongo_conn:stop(Conn)
        end, queue:to_list(Conns)),
        {ok, lists:keydelete(PoolId, 1, Pools)}
    end,
  {reply, Result, State#state{pools=Pools1}};

handle_call({conn, PoolId}, From, State) ->
  handle_call({conn, PoolId, 1}, From, State);
handle_call({conn, PoolId, NumReqs}, _From, #state{pools = Pools} = State) ->
  % With the new wire protocol, NumReqs should always be 1 since we no longer need to issue the getLastError call in
  % tandum with the actual call.  However, the unit tests need this extra clause to test rolling over the request ID.
  case get_pool(PoolId, Pools) of
    undefined ->
      {reply, {undefined, undefined}, State};
    {Pool, Others} ->
      case queue:out(Pool#pool.conns) of
        {{value, Conn}, Q2} ->
          TNextReq = ((Pool#pool.req_id) + NumReqs),
          %if we rollover our requests number, then we will not be able to match up the
          %response with the caller and not perform a gen_server:reply in emongo_conn:process_bin
          {RetPool, Pool1} = case TNextReq >= 16#80000000 of % Signed, 32-bit max, used by emongo_packet.erl
            true ->
              {
                Pool#pool{conns = queue:in(Conn, Q2), req_id = 1},
                Pool#pool{conns = queue:in(Conn, Q2), req_id = 1 + NumReqs}
              };
            false ->
              {
                Pool,
                Pool#pool{conns = queue:in(Conn, Q2), req_id = TNextReq}
              }
          end,
          Pools1 = [{PoolId, Pool1} | Others],
          {reply, {Conn, RetPool}, State#state{pools=Pools1}};
        {empty, _} ->
          {reply, {undefined, Pool}, State}
      end
  end;

handle_call({authorize_new_dbs, PoolId}, _From, #state{pools = Pools} = State) ->
  case get_pool(PoolId, Pools) of
    undefined ->
      {reply, undefined, State};
    {Pool, Others} ->
      NewPool = lists:foldl(fun(Conn, PoolAcc) ->
        do_auth(Conn, PoolAcc)
      end, Pool, queue:to_list(Pool#pool.conns)),
      {reply, ok, State#state{pools = [{PoolId, NewPool} | Others]}}
  end;

handle_call(_, _From, State) -> {reply, {error, invalid_call}, State}.

%--------------------------------------------------------------------
% Function: handle_cast(Msg, State) -> {noreply, State} |
%                    {noreply, State, Timeout} |
%                    {stop, Reason, State}
% Description: Handling cast messages
%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
  {noreply, State}.

%--------------------------------------------------------------------
% Function: handle_info(Info, State) -> {noreply, State} |
%                     {noreply, State, Timeout} |
%                     {stop, Reason, State}
% Description: Handling all non call/cast messages
%--------------------------------------------------------------------
handle_info({'EXIT', _, shutdown}, State) ->
  {noreply, State};

handle_info({'EXIT', Pid, Error}, #state{pools = Pools} = State) ->
  ?ERROR("EXIT ~p, ~p in ~p~n", [Pid, Error, ?MODULE]),
  NewPools = cleanup_pid(Pid, Pools),
  {noreply, State#state{pools = NewPools}};

handle_info(Info, State) ->
  ?WARN("Unrecognized message in ~p: ~p~n", [?MODULE, Info]),
  {noreply, State}.

%--------------------------------------------------------------------
% Function: terminate(Reason, State) -> void()
% Description: This function is called by a gen_server when it is about to
% terminate. It should be the opposite of Module:init/1 and do any necessary
% cleaning up. When it returns, the gen_server terminates with Reason.
% The return value is ignored.
%--------------------------------------------------------------------
terminate(_Reason, _State) ->
  ok.

%--------------------------------------------------------------------
% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
% Description: Convert process state when code is changed
%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%--------------------------------------------------------------------
%% Internal functions
%--------------------------------------------------------------------
initialize_pools() ->
  case application:get_env(emongo, pools) of
    undefined ->
      [];
    {ok, Pools} ->
      [begin
        Host            = proplists:get_value(host,     Options, "localhost"),
        Port            = proplists:get_value(port,     Options, 27017),
        DefaultDatabase = proplists:get_value(database, Options, "test"),
        Size            = proplists:get_value(size,     Options, 1),
        Pool            = create_pool(PoolId, Host, Port, DefaultDatabase, Size, Options),
        {PoolId, do_open_connections(Pool)}
       end || {PoolId, Options} <- Pools]
  end.

do_open_connections(#pool{id                  = PoolId,
                          host                = Host,
                          port                = Port,
                          size                = Size,
                          socket_options      = SocketOptions,
                          conns               = Conns} = Pool) ->
  case queue:len(Conns) < Size of
    true ->
      % The emongo_conn:start_link function will throw an exception if it is unable to connect.
      {ok, Conn} = emongo_conn:start_link(PoolId, Host, Port, SocketOptions),
      NewPool = do_auth(Conn, Pool),
      do_open_connections(NewPool#pool{conns = queue:in(Conn, Conns)});
    false -> Pool
  end.

cleanup_pid(_Pid, []) -> ok; % Pid not found; no cleanup necessary
cleanup_pid(Pid, [{PoolId, Pool} | Others]) ->
  FilConns = queue:filter(fun(Conn) -> emongo_conn:write_pid(Conn) =/= Pid end, Pool#pool.conns),
  case queue:len(FilConns) == queue:len(Pool#pool.conns) of
    true  ->
      [{PoolId, Pool} | cleanup_pid(Pid, Others)];
    false ->
      FilPool = Pool#pool{conns = FilConns},
      NewPool = do_open_connections(FilPool),
      [{PoolId, NewPool} | Others]
  end.

pass_hash(undefined, undefined) -> undefined;
pass_hash(User, Pass) ->
  emongo:dec2hex(erlang:md5(<<User/binary, ":mongo:", Pass/binary>>)).

do_auth(_Conn, #pool{user = undefined, pass_hash = undefined} = Pool) -> Pool;
do_auth(Conn, #pool{auth_db = AuthDatabase} = Pool) when AuthDatabase =/= undefined ->
  do_auth([AuthDatabase], Conn, Pool);
do_auth(Conn, #pool{id = PoolId, database = DefaultDatabase} = Pool) ->
  % Get all coll_to_db maps where AuthFlag is true
  CollToDbs = [DB || {DB, true} <- match_config(PoolId, {coll_to_db, '_'})],
  RegisteredDBs = [DefaultDatabase | CollToDbs],
  UniqueDBs = lists:usort(RegisteredDBs),
  do_auth(UniqueDBs, Conn, Pool).

do_auth([], _Conn, Pool) -> Pool;
do_auth([DB | Others], Conn, #pool{user = User, pass_hash = PassHash} = Pool) ->
  Bytes = crypto:strong_rand_bytes(6),
  ClientNonce = base64:encode(emongo:dec2hex(Bytes)),

  % Client initiates SCRAM auth session with username and a random number (ClientNonce)
  FirstBare = <<"n=", User/binary, ",r=", ClientNonce/binary>>,
  Payload1 = <<"n,,", FirstBare/binary>>,
  Query1 = #emo_query{q=[{<<"saslStart">>, 1},
                         {<<"mechanism">>, <<"SCRAM-SHA-1">>},
                         {<<"payload">>, base64:encode(Payload1)},
                         {<<"autoAuthorize">>, 1}], limit=1},
  {RespDoc1, RespPayload1} = scram_authorize_conn(DB, Pool, Query1, Conn),
  ConId = proplists:get_value(<<"conversationId">>, RespDoc1),
  EncodedPayload = proplists:get_value(<<"payload">>, RespDoc1),
  ServerFirst = base64:decode(EncodedPayload),

  % Server issues a challenge and Client responds with a proof
  Iterations = proplists:get_value(<<"i">>, RespPayload1),
  Salt = proplists:get_value(<<"s">>, RespPayload1),
  Rnonce = proplists:get_value(<<"r">>, RespPayload1),
  Iter = list_to_integer(binary_to_list(Iterations)),
  WithoutProof = <<"c=biws,r=", Rnonce/binary>>,
  SaltedPass  = salt_password(PassHash, base64:decode(Salt), Iter),
  ClientKey = crypto:hmac(sha, SaltedPass, <<"Client Key">>),
  StoredKey = crypto:hash(sha, ClientKey),
  AuthMsg = <<FirstBare/binary, ",", ServerFirst/binary, ",", WithoutProof/binary>>,
  ClientSig = crypto:hmac(sha, StoredKey, AuthMsg),
  XorVal = base64:encode(crypto:exor(ClientKey, ClientSig)),
  Payload2 = <<WithoutProof/binary, ",p=", XorVal/binary>>,
  Query2 = #emo_query{q=[{<<"saslContinue">>, 1},
                         {<<"conversationId">>, ConId},
                         {<<"payload">>, base64:encode(Payload2)}], limit=1},
  {RespDoc2, RespPayload2} = scram_authorize_conn(DB, Pool, Query2, Conn),

  % Client verifies the server's proof
  ServerKey = crypto:hmac(sha, SaltedPass, <<"Server Key">>),
  ServerSig = base64:encode(crypto:hmac(sha, ServerKey, AuthMsg)),
  ServerSigResponse = proplists:get_value(<<"v">>, RespPayload2),
  if ServerSigResponse =/= ServerSig ->
    throw({emongo_authentication_failed, <<"Server proof could not be verified">>});
    true -> ok
  end,
  case proplists:get_bool(<<"done">>, RespDoc2) of
    false -> Noop = #emo_query{q=[{<<"saslContinue">>, 1},
                                  {<<"conversationId">>, ConId},
                                  {<<"payload">>, base64:encode(<<"">>)}], limit=1},
             authorize_conn_for_db(DB, Pool, Noop, Conn);
    true  -> ok
  end,
  do_auth(Others, Conn, Pool#pool{req_id = Pool#pool.req_id + 1}).

authorize_conn_for_db(DB, Pool, Query, Conn) ->
  Packet = emongo_packet:do_query(DB, "$cmd", Pool#pool.req_id, Query),
  Resp = send_recv_command(do_auth, "$cmd", undefined, [], Conn, Pool, Packet),
  case response_ok(Resp) of
    true  -> ok;
    false -> throw({emongo_authentication_failed, error_msg(Resp)})
  end.

scram_authorize_conn(DB, Pool, Query, Conn) ->
  Packet = emongo_packet:do_query(DB, "$cmd", Pool#pool.req_id, Query),
  Resp = send_recv_command(scram_authorize_conn, "$cmd", undefined, [], Conn, Pool, Packet),
  case response_ok(Resp) of
    true ->
      Doc = response_first_doc(Resp),
      EncodedPayload = proplists:get_value(<<"payload">>, Doc),
      Payload = parse_scram_response(base64:decode(EncodedPayload)),
      {Doc, Payload};
    false ->
      throw({emongo_authentication_failed, error_msg(Resp)})
  end.

parse_scram_response(Resp) ->
  lists:map(fun(X) ->
    case re:split(X, "=", [{return, binary}, {parts, 2}]) of
      [Key, Value] -> {Key, Value};
                 _ -> {undefined,  undefined}
    end
  end, re:split(Resp, <<",">>)).

salt_password(Password, Salt, Iterations) ->
  salt_password(Password, Salt, Iterations, 1, []).

salt_password(Password, Salt, Iterations, BlockIndex, Acc) ->
  DigestLength = ?SHA1_DIGEST_LEN,
  case iolist_size(Acc) > DigestLength of
    true ->
      <<Bin:DigestLength/binary, _/binary>> = iolist_to_binary(lists:reverse(Acc)),
      Bin;
    false ->
      Block = sha_hash_block(Password, Salt, Iterations, BlockIndex, 1, <<>>, <<>>),
      salt_password(Password, Salt, Iterations, BlockIndex + 1, [Block | Acc])
  end.

sha_hash_block(_Password, _Salt, Iterations, _BlockIndex, Iteration, _PrevBlock, Acc) when Iteration > Iterations ->
  Acc;

sha_hash_block(Password, Salt, Iterations, BlockIndex, 1, _PrevBlock, _Acc) ->
  InitCtx = crypto:hmac_init(sha, Password),
  NewCtx = crypto:hmac_update(InitCtx, <<Salt/binary, BlockIndex:32/integer>>),
  InitialBlock = crypto:hmac_final(NewCtx),
  sha_hash_block(Password, Salt, Iterations, BlockIndex, 2, InitialBlock, InitialBlock);

sha_hash_block(Password, Salt, Iterations, BlockIndex, Iteration, PrevBlock, Acc) ->
  InitCtx = crypto:hmac_init(sha, Password),
  NewCtx = crypto:hmac_update(InitCtx, PrevBlock),
  NextBlock = crypto:hmac_final(NewCtx),
  sha_hash_block(Password, Salt, Iterations, BlockIndex, Iteration + 1, NextBlock, crypto:exor(NextBlock, Acc)).

get_pool(PoolId, Pools) ->
  get_pool(PoolId, Pools, []).

get_pool(_, [], _) ->
  undefined;

get_pool(PoolId, [{PoolId, Pool}|Tail], Others) ->
  {Pool, lists:append(Tail, Others)};

get_pool(PoolId, [Pool|Tail], Others) ->
  get_pool(PoolId, Tail, [Pool|Others]).

get_database(#pool{id = PoolId} = Pool, Collection) ->
  case get_config(PoolId, {coll_to_db, to_binary(Collection)}) of
    {Database, true}  -> Database;
    {Database, false} ->
      % If an auth db was given, assume that the user has roles to access all other dbs
      case Pool#pool.auth_db of
        undefined -> throw({emongo_db_not_authenticated, Database});
        _         -> Database
      end;
    _ -> Pool#pool.database
  end.

dec2hex(Dec) ->
  dec2hex(<<>>, Dec).

dec2hex(N, <<I:8,Rem/binary>>) ->
  dec2hex(<<N/binary, (hex0((I band 16#f0) bsr 4)):8,
            (hex0((I band 16#0f))):8>>, Rem);
dec2hex(N,<<>>) ->
  N.

hex2dec(Hex) when is_list(Hex) ->
  hex2dec(list_to_binary(Hex));

hex2dec(Hex) ->
  hex2dec(<<>>, Hex).

hex2dec(N,<<A:8,B:8,Rem/binary>>) ->
  hex2dec(<<N/binary, ((dec0(A) bsl 4) + dec0(B)):8>>, Rem);
hex2dec(N,<<>>) ->
  N.

utf8_encode(Value) ->
  try
  iolist_to_binary(Value)
  catch _:_ ->
  case unicode:characters_to_binary(Value) of
    {error, Bin, RestData} ->
      exit({emongo_cannot_convert_chars_to_binary, Value, Bin, RestData});
    {incomplete, Bin1, Bin2} ->
      exit({emongo_cannot_convert_chars_to_binary, Value, Bin1, Bin2});
    EncodedValue -> EncodedValue
  end
  end.

% create_cmd(Command, Collection, ExtraParams, ForcedLimit, Options)
create_cmd(Command, Collection, ExtraParams, undefined, Options) ->
  EmoQuery = transform_options(Options, #emo_query{}),
  EmoQuery#emo_query{q = [{Command, Collection} | ExtraParams ++ EmoQuery#emo_query.q]};
create_cmd(Command, Collection, ExtraParams, ForcedLimit, Options) ->
  NewOptions = [{limit, ForcedLimit} | proplists:delete(limit, Options)],
  create_cmd(Command, Collection, ExtraParams, undefined, NewOptions).

create_query(Options, SelectorIn) ->
  Selector = transform_selector(SelectorIn),
  EmoQuery = transform_options(Options, #emo_query{}),
  finalize_emo_query(Selector, EmoQuery).

finalize_emo_query(Selector, #emo_query{q = []} = EmoQuery) -> EmoQuery#emo_query{q = Selector};
finalize_emo_query(Selector, #emo_query{q = Q}  = EmoQuery) ->
  EmoQuery#emo_query{q = [{<<"$query">>, {struct, Selector}} | Q]}.

transform_options([], EmoQuery) ->
  EmoQuery;
transform_options([{limit, Limit} | Rest], EmoQuery) ->
  NewEmoQuery = EmoQuery#emo_query{limit=Limit},
  transform_options(Rest, NewEmoQuery);
transform_options([{offset, Offset} | Rest], EmoQuery) ->
  NewEmoQuery = EmoQuery#emo_query{offset=Offset},
  transform_options(Rest, NewEmoQuery);
transform_options([{orderby, OrderbyIn} | Rest],
                  #emo_query{q = Query} = EmoQuery) ->
  Orderby = {<<"$orderby">>, {struct, [{Key, case Dir of desc -> -1; -1 -> -1; _ -> 1 end} ||
                                       {Key, Dir} <- OrderbyIn]}},
  NewEmoQuery = EmoQuery#emo_query{q = [Orderby | Query]},
  transform_options(Rest, NewEmoQuery);
transform_options([{fields, Fields} | Rest], EmoQuery) ->
  NewEmoQuery = EmoQuery#emo_query{field_selector=convert_fields(Fields)},
  transform_options(Rest, NewEmoQuery);
transform_options([Opt | Rest], #emo_query{opts = OptsIn} = EmoQuery)
    when is_integer(Opt) ->
  NewEmoQuery = EmoQuery#emo_query{opts = Opt bor OptsIn},
  transform_options(Rest, NewEmoQuery);
transform_options([{<<_/binary>>, _} = Option | Rest], #emo_query{q = Query} = EmoQuery) ->
  NewEmoQuery = EmoQuery#emo_query{q = [Option | Query]},
  transform_options(Rest, NewEmoQuery);
transform_options([{AllowedBinaries, Val} | Rest], #emo_query{q = Query} = EmoQuery)
    when AllowedBinaries == new;
         AllowedBinaries == sort;
         AllowedBinaries == remove;
         AllowedBinaries == upsert ->
  NewEmoQuery = EmoQuery#emo_query{q = [{to_binary(AllowedBinaries), Val} | Query]},
  transform_options(Rest, NewEmoQuery);
transform_options([{Ignore, _} | Rest], EmoQuery)
    when Ignore == timeout;
         % The write-concern options are handled in sync_command()
         Ignore == write_concern;
         Ignore == write_concern_timeout;
         Ignore == journal_write_ack;
         Ignore == ordered ->
  transform_options(Rest, EmoQuery);
transform_options([Ignore | Rest], EmoQuery)
    when Ignore == response_options ->
  transform_options(Rest, EmoQuery);
transform_options([Invalid | _Rest], _EmoQuery) ->
  throw({emongo_invalid_option, Invalid}).

transform_selector({struct, Selector}) ->
  transform_selector(Selector);
transform_selector(Selector) ->
  lists:map(fun(undefined)    -> undefined;
               ({Key, Value}) ->
    ConvKey = convert_key(Key),
    ForceDataType = force_data_type(ConvKey),
    ConvValue = convert_value(ForceDataType, Value),
    {ConvKey, ConvValue}
  end, Selector).

convert_key(Bin)  when is_binary(Bin) -> Bin;
convert_key(List) when is_list(List)  -> List;
convert_key(oid)       -> oid;
convert_key('>')       -> <<"$gt">>;
convert_key(gt)        -> <<"$gt">>;
convert_key('>=')      -> <<"$gte">>;
convert_key(gte)       -> <<"$gte">>;
convert_key('<')       -> <<"$lt">>;
convert_key(lt)        -> <<"$lt">>;
convert_key('=<')      -> <<"$lte">>;
convert_key(lte)       -> <<"$lte">>;
convert_key('=/=')     -> <<"$ne">>;
convert_key('/=')      -> <<"$ne">>;
convert_key(ne)        -> <<"$ne">>;
convert_key(in)        -> <<"$in">>;
convert_key(nin)       -> <<"$nin">>;
convert_key(mod)       -> <<"$mod">>;
convert_key(all)       -> <<"$all">>;
convert_key(size)      -> <<"$size">>;
convert_key(exists)    -> <<"$exists">>;
convert_key(near)      -> <<"$near">>;
convert_key(where)     -> <<"$where">>;
convert_key(elemMatch) -> <<"$elemMatch">>;
convert_key(maxDistance) -> <<"$maxDistance">>;
convert_key(geoWithin) -> <<"$geoWithin">>;
convert_key(centerSphere) -> <<"$centerSphere">>;
convert_key(box)       -> <<"$box">>;
convert_key(center)    -> <<"$center">>;
convert_key(regex)     -> <<"$regex">>.

force_data_type(<<"$in">>)  -> array;
force_data_type(<<"$nin">>) -> array;
force_data_type(<<"$mod">>) -> array;
force_data_type(<<"$all">>) -> array;
force_data_type(<<"$or">>)  -> array;
force_data_type(<<"$and">>) -> array;
force_data_type(_)          -> undefined.

convert_value(array, {array, Vals}) ->
  {array, [convert_value(undefined, V) || V <- Vals]};
convert_value(array, Vals) ->
  convert_value(array, {array, Vals});
convert_value(_, Sel) when ?IS_DOCUMENT(Sel) ->
  transform_selector(Sel);
convert_value(_, [SubSel | _] = SubSels) when ?IS_DOCUMENT(SubSel) ->
  {array, [transform_selector(Sel) || Sel <- SubSels]};
convert_value(_, {array, [SubSel | _] = SubSels}) when ?IS_DOCUMENT(SubSel) ->
  {array, [transform_selector(Sel) || Sel <- SubSels]};
convert_value(_, Value) -> Value.

% This function strips the input selector.  All values are replaced with 'undefined' while preserving the structure and
% keys in the selector.  This is useful for tools that track queries to map them to indexes on the collection.
strip_selector([]) -> undefined;
strip_selector({struct, Selector}) -> strip_selector(Selector);
strip_selector(Selector) ->
  ConvSel = lists:map(fun({Key, Value}) ->
    % Some operations include a sub-document as the Value, such as $elemMatch, $and, $or, etc.  However, others do not,
    % such as $in, $exists, $lte, etc.  In the former case, we need to recursively strip the selector.  In the latter
    % case, we can just replace this Key/Value with 'undefined'.
    ConvKey = to_binary(convert_key(Key)),
    ConvVal = strip_value(Value),
    case (binary:first(ConvKey) == $$) and (ConvVal == undefined) of
      true -> undefined;
      _    -> {ConvKey, ConvVal}
    end
  end, Selector),
  case lists:filter(fun(undefined) -> false; (_) -> true end, ConvSel) of
    []  -> undefined;
    Res -> Res
  end.

strip_value(Sel) when ?IS_DOCUMENT(Sel) ->
  strip_selector(Sel);
strip_value([SubSel | _] = SubSels) when ?IS_DOCUMENT(SubSel) ->
  {array, [strip_selector(Sel) || Sel <- SubSels]};
strip_value({array, [SubSel | _] = SubSels}) when ?IS_DOCUMENT(SubSel) ->
  {array, [strip_selector(Sel) || Sel <- SubSels]};
strip_value(_Value) -> undefined.

dec0($a) ->  10;
dec0($b) ->  11;
dec0($c) ->  12;
dec0($d) ->  13;
dec0($e) ->  14;
dec0($f) ->  15;
dec0(X)  ->  X - $0.

hex0(10) -> $a;
hex0(11) -> $b;
hex0(12) -> $c;
hex0(13) -> $d;
hex0(14) -> $e;
hex0(15) -> $f;
hex0(I)  -> $0 + I.

send_recv_command(Command, Collection, Selector, Options, Conn, #pool{id = PoolId, req_id = ReqId}, Packet) ->
  try
    time_call({Command, Collection, Selector, Options}, fun() ->
      emongo_conn:send_recv(Conn, ReqId, Packet, get_timeout(PoolId, Options))
    end)
  catch _:{emongo_conn_error, Error} ->
    throw({emongo_conn_error, Error, Command, Collection, Selector,
           [{options, Options}, {msg_queue_len, emongo_conn:queue_lengths(Conn)}]})
  end.

send_command(Command, Collection, Selector, Options, Conn, #pool{id = PoolId, req_id = ReqId}, Packet) ->
  try
    time_call({Command, Collection, Selector, Options}, fun() ->
      emongo_conn:send(Conn, ReqId, Packet, get_timeout(PoolId, Options))
    end)
  catch _:{emongo_conn_error, Error} ->
    throw({emongo_conn_error, Error, Command, Collection, Selector,
           [{options, Options}, {msg_queue_len, emongo_conn:queue_lengths(Conn)}]})
  end.

% TODO: Include selector in emongo_error messages.
get_sync_result(Resp, Options) ->
  case lists:member(response_options, Options) of
    true  -> Resp;
    false -> get_sync_result(Resp)
  end.

get_sync_result(Resp) ->
  Doc = response_first_doc(Resp),
  % Check the root-level of the response document.
  assert_resp_code(Doc),
  % Check writeErrors.
  case proplists:get_value(<<"writeErrors">>, Doc, undefined) of
    undefined -> ok;
    {array, Errors} ->
      lists:foreach(fun(Error) -> assert_resp_code(Error) end, Errors),
      throw({emongo_error, Errors})
  end,
  % Check writeConcernError.
  case proplists:get_value(<<"writeConcernError">>, Doc, undefined) of
    undefined -> ok;
    Error     -> throw({emongo_error, Error})
  end,
  % Check for an "ok" response.
  case response_ok(Resp) of
    true  -> ok;
    false -> throw({emongo_error, {invalid_response, Resp}})
  end.

assert_resp_code(Doc) ->
  case proplists:get_value(<<"code">>, Doc, undefined) of
    undefined -> ok;
    0         -> ok;
    11000     -> throw({emongo_error, duplicate_key, error_msg(Doc)});
    11001     -> throw({emongo_error, duplicate_key, error_msg(Doc)});
    13        -> throw({emongo_authorization_error, error_msg(Doc)});
    Code      -> throw({emongo_error, {Code, error_msg(Doc)}})
  end.

time_call({Command, Collection, Selector, _Options}, Fun) ->
  {TimeUsec, Res} = timer:tc(fun() ->
    try
      Fun()
    catch C:E:S ->
      {exception, C, E, S}
    end
  end),
  PreviousTiming = case erlang:get(?TIMING_KEY) of
    undefined                     -> [];
    T when length(T) < ?MAX_TIMES -> T;
    T                             -> tl(T)
  end,
  CurTimeInfo = [
    {time_usec, TimeUsec},
    {cmd,       Command},
    {coll,      Collection},
    {sel,       Selector}],
  erlang:put(?TIMING_KEY, PreviousTiming ++ [CurTimeInfo]),
  case Res of
    {exception, C, E, Stacktrace} -> erlang:raise(C, E, Stacktrace);
    _                             -> Res
  end.

get_timeout(PoolId, Options) ->
  proplists:get_value(timeout, Options, get_config(PoolId, timeout)).

get_ordered_option(Ordered) when is_boolean(Ordered) ->
  {<<"ordered">>, Ordered};
get_ordered_option(Options) ->
  {<<"ordered">>, proplists:get_value(ordered, Options, true)}.

get_writeconcern_option(PoolId, Options) ->
  {<<"writeConcern">>, {struct, [
    {<<"w">>,        proplists:get_value(write_concern,         Options, get_config(PoolId, write_concern))},
    {<<"j">>,        proplists:get_value(journal_write_ack,     Options, get_config(PoolId, journal_write_ack))},
    {<<"wtimeout">>, proplists:get_value(write_concern_timeout, Options, get_config(PoolId, write_concern_timeout))}
  ]}}.

to_binary(undefined)           -> undefined;
to_binary(V) when is_binary(V) -> V;
to_binary(V) when is_list(V)   -> list_to_binary(V);
to_binary(V) when is_atom(V)   -> list_to_binary(atom_to_list(V)).

to_list(undefined)           -> undefined;
to_list(V) when is_list(V)   -> V;
to_list(V) when is_binary(V) -> binary_to_list(V);
to_list(V) when is_atom(V)   -> atom_to_list(V).

cur_time_ms() ->
  os:system_time(milli_seconds).
% Note: use the following code if you need compatibility with Erlang < 18.1
%  {MegaSec, Sec, MicroSec} = os:timestamp(),
%  MegaSec * 1000000000 + Sec * 1000 + erlang:round(MicroSec / 1000).

convert_fields([])                    -> [];
convert_fields([{Field, Val} | Rest]) -> [{Field, Val} | convert_fields(Rest)];
convert_fields([Field | Rest])        -> [{Field, 1}   | convert_fields(Rest)].

set_read_preference(PoolId, Options) ->
  ReadPref = proplists:get_value(read_pref, Options, get_config(PoolId, default_read_pref)),
  set_read_preference_int(to_binary(ReadPref), proplists:delete(read_pref, Options)).

set_read_preference_int(undefined, Options) -> Options;
set_read_preference_int(ReadPref,  Options) ->
  [{<<"$readPreference">>, [{<<"mode">>, ReadPref}]} | Options].

merge_insert_sync_page_response(undefined, Resp = #response{documents = [_Doc]}) ->
  % The accumulator is the default one, so this is the first response.
  Resp;
merge_insert_sync_page_response(AccResp = #response{documents = [AccDoc]}, Resp = #response{documents = [Doc]}) ->
  NewDoc =
    [
      {<<"ok">>, case response_ok(AccResp) and response_ok(Resp) of true -> 1.0; _ -> 0.0 end},
      {<<"n">>,  response_n(AccResp) + response_n(Resp)}
    ] ++
    combine_write_errors(<<"writeErrors">>,       AccDoc, Doc) ++
    combine_write_errors(<<"writeConcernError">>, AccDoc, Doc),
  AccResp#response{documents = [NewDoc]};
merge_insert_sync_page_response(_, Resp) ->
  throw({emongo_error, {invalid_response, Resp}}).

combine_write_errors(Key, AccDoc, Doc) ->
  combine_write_error_lists(Key, proplists:get_value(Key, AccDoc), proplists:get_value(Key, Doc)).

% This function handles the writeErrors key, which maps to {array, [e1, e2, ...]}
% but also the writeConcernError key, which maps to a single error document.
combine_write_error_lists(_, undefined, undefined) ->
  [];
combine_write_error_lists(Key, {array, AccRespWriteErrors}, undefined) ->
  [{Key, {array, AccRespWriteErrors}}];
combine_write_error_lists(Key, undefined, {array, RespWriteErrors}) ->
  [{Key, {array, RespWriteErrors}}];
combine_write_error_lists(Key, undefined, RespWriteError) ->
  [{Key, {array, [RespWriteError]}}];
combine_write_error_lists(Key, {array, AccRespWriteErrors}, {array, RespWriteErrors}) ->
  [{Key, {array, AccRespWriteErrors ++ RespWriteErrors}}];
combine_write_error_lists(Key, {array, AccRespWriteErrors}, RespWriteError) ->
  [{Key, {array, AccRespWriteErrors ++ [RespWriteError]}}].

% Get the next page of documents when calling a bulk function
next_bulk_page(_PoolId, []) ->
  {[], []};
next_bulk_page(PoolId, Documents) ->
  MaxBatchSize = case get_config(PoolId, max_batch_size) of
    0   -> 1000;
    MBS -> MBS
  end,
  % TODO: MongoDB limits bulk inserts to 1,000 in 3.4 and bumped that limit up to 100,000 for 3.6.  However, there is
  %       also a 16 MB limit on the message size.  That 16 MB limit is not enforced here.
  case length(Documents) > MaxBatchSize of
    true -> lists:split(MaxBatchSize, Documents);
    _    -> {Documents, []}
  end.

response_ok(#response{documents = [Doc]}) ->
  case proplists:get_value(<<"ok">>, Doc, undefined) of
    1    -> true;
    1.0  -> true;
    _    -> false
  end;
response_ok(Resp) -> throw({emongo_error, {invalid_response, Resp}}).

error_msg([_|_] = Doc)                  -> proplists:get_value(<<"errmsg">>, Doc, undefined);
error_msg(#response{documents = [Doc]}) -> error_msg(Doc);
error_msg(Resp)                         -> throw({emongo_error, {invalid_response, Resp}}).

response_docs(#response{documents = Docs}) -> Docs;
response_docs(Resp)                        -> throw({emongo_error, {invalid_response, Resp}}).

response_first_doc(Resp) -> hd(response_docs(Resp)).

response_n(Resp) ->
  round(proplists:get_value(<<"n">>, response_first_doc(Resp), 0)).

-define(EMONGO_CONFIG, emongo_config).
init_config()                     -> ets:new(?EMONGO_CONFIG, [public, named_table, {read_concurrency, true}]).
get_config(PoolId, Name)          -> get_config(PoolId, Name, undefined).
get_config(PoolId, Name, Default) -> try ets:lookup_element(?EMONGO_CONFIG, {PoolId, Name}, 2) catch _:_ -> Default end.
save_config(PoolId, Name, Value)  -> ets:insert(?EMONGO_CONFIG, {{PoolId, Name}, Value}).
match_config(PoolId, NameMatch)   -> [Value || [Value] <- ets:match(?EMONGO_CONFIG, {{PoolId, NameMatch}, '$1'})].
