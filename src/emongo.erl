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
         pools/0, add_pool/5, add_pool/6, remove_pool/1,
         queue_lengths/0,
         register_collections_to_databases/2,
         find/3, find/4,
         find_all/2, find_all/3, find_all/4,
         get_more/4, get_more/5,
         find_one/3, find_one/4,
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
         strip_selector/1]).

-define(TIMING_KEY, emongo_timing).
-define(MAX_TIMES,  10).
-define(EMONGO_CONFIG, emongo_config).
-define(SHA1_DIGEST_LEN, 20).
-define(WRITE_CMD_LIMIT, 1000).
-define(IS_MONGO_2_6(MaxWireVersion), (MaxWireVersion >= 3)).
-define(IS_MONGO_3_2(MaxWireVersion), (MaxWireVersion >= 4)).

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

add_pool(PoolId, Host, Port, DefaultDatabase, Size, Options) ->
  DefaultReadPref = to_binary(proplists:get_value(default_read_pref, Options, undefined)),
  ets:insert(?EMONGO_CONFIG, {{default_read_pref, PoolId}, DefaultReadPref}),
  gen_server:call(?MODULE, {add_pool, create_pool(PoolId, Host, Port, DefaultDatabase, Size, Options)}, infinity).

create_pool(PoolId, Host, Port, DefaultDatabase, Size, Options) ->
  Def = #pool{},
  Timeout             = proplists:get_value(timeout,               Options, Def#pool.timeout),
  AuthDatabase        = proplists:get_value(auth_db,               Options, undefined),
  User      = to_binary(proplists:get_value(user,                  Options, Def#pool.user)),
  Password  = to_binary(proplists:get_value(password,              Options, undefined)),
  MaxPipelineDepth    = proplists:get_value(max_pipeline_depth,    Options, Def#pool.max_pipeline_depth),
  SocketOptions       = proplists:get_value(socket_options,        Options, Def#pool.socket_options),
  WriteConcern        = proplists:get_value(write_concern,         Options, Def#pool.write_concern),
  WriteConcernTimeout = proplists:get_value(write_concern_timeout, Options, Def#pool.write_concern_timeout),
  DisconnectTimeouts  = proplists:get_value(disconnect_timeouts,   Options, Def#pool.disconnect_timeouts),
  #pool{id                    = PoolId,
        host                  = to_list(Host),
        port                  = Port,
        database              = DefaultDatabase,
        size                  = Size,
        timeout               = Timeout,
        auth_db               = AuthDatabase,
        user                  = User,
        pass_hash             = pass_hash(User, Password),
        max_pipeline_depth    = MaxPipelineDepth,
        socket_options        = SocketOptions,
        write_concern         = WriteConcern,
        write_concern_timeout = WriteConcernTimeout,
        disconnect_timeouts   = DisconnectTimeouts}.

remove_pool(PoolId) ->
  gen_server:call(?MODULE, {remove_pool, PoolId}).

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
  Ets = lists:map(fun
    ({Collection, Database})           -> {{coll_to_db, PoolId, to_binary(Collection)}, Database, true};
    ({Collection, Database, AuthFlag}) -> {{coll_to_db, PoolId, to_binary(Collection)}, Database, AuthFlag}
  end, CollDbMap),
  ets:insert(?EMONGO_CONFIG, Ets),
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
%     Limit = integer
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
find(PoolId, Collection, Selector) -> find(PoolId, Collection, Selector, []).

find(PoolId, Collection, Selector, OptionsIn) when ?IS_DOCUMENT(Selector),
                                                   is_list(OptionsIn) ->
  {Conn, Pool} = gen_server:call(?MODULE, {conn, PoolId}, infinity),
  Options      = [{<<"$maxTimeMS">>, get_timeout(OptionsIn, Pool)} | set_read_preference(PoolId, OptionsIn)],
  Query        = create_query(Options, Selector),
  Packet       = emongo_packet:do_query(get_database(Pool, Collection), Collection,
                                  Pool#pool.req_id, Query),
  Resp = send_recv_command(find, Collection, Selector, Options, Conn, Pool, Packet),
  case lists:member(response_options, Options) of
    true -> Resp;
    false -> Resp#response.documents
  end.

%------------------------------------------------------------------------------
% find_all
%------------------------------------------------------------------------------
find_all(PoolId, Collection) ->
  find_all(PoolId, Collection, [], []).

find_all(PoolId, Collection, Selector) when ?IS_DOCUMENT(Selector) ->
  find_all(PoolId, Collection, Selector, []).

find_all(PoolId, Collection, Selector, Options) when ?IS_DOCUMENT(Selector),
                                                     is_list(Options) ->
  Resp = find(PoolId, Collection, Selector, [response_options|Options]),
  find_all(PoolId, Collection, Selector, Options, Resp).

find_all(_PoolId, _Collection, _Selector, Options, Resp)
    when is_record(Resp, response), Resp#response.cursor_id == 0 ->
  case lists:member(response_options, Options) of
    true -> Resp;
    false -> Resp#response.documents
  end;

find_all(PoolId, Collection, Selector, Options, Resp)
    when is_record(Resp, response) ->
  Resp1 = get_more(PoolId, Collection, Resp#response.cursor_id, 0, Options),
  Documents = lists:append(Resp#response.documents, Resp1#response.documents),
  find_all(PoolId, Collection, Selector, Options, Resp1#response{documents=Documents}).

%------------------------------------------------------------------------------
% find_one
%------------------------------------------------------------------------------
find_one(PoolId, Collection, Selector) when ?IS_DOCUMENT(Selector) ->
  find_one(PoolId, Collection, Selector, []).

find_one(PoolId, Collection, Selector, Options) when ?IS_DOCUMENT(Selector),
                                                     is_list(Options) ->
  Options1 = [{limit, -1} | lists:keydelete(limit, 1, Options)],
  find(PoolId, Collection, Selector, Options1).

%------------------------------------------------------------------------------
% get_more
%------------------------------------------------------------------------------
get_more(PoolId, Collection, CursorID, NumToReturn) -> get_more(PoolId, Collection, CursorID, NumToReturn, []).
get_more(PoolId, Collection, CursorID, NumToReturn, Options) ->
  {Conn, Pool} = gen_server:call(?MODULE, {conn, PoolId}, infinity),
  Packet = emongo_packet:get_more(get_database(Pool, Collection), Collection,
                                  Pool#pool.req_id, NumToReturn, CursorID),
  send_recv_command(get_more, Collection, CursorID, [{num_to_return, NumToReturn} | Options], Conn, Pool, Packet).

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

insert_sync(PoolId, Collection, DocumentsIn, Options) ->
  Documents = if
    ?IS_LIST_OF_DOCUMENTS(DocumentsIn) -> DocumentsIn;
    ?IS_DOCUMENT(DocumentsIn)          -> [DocumentsIn]
  end,
  {Conn, Pool} = gen_server:call(?MODULE, {conn, PoolId, 2}, infinity),
  if ?IS_MONGO_2_6(Pool#pool.max_wire_version) ->
       {Head, Tail} = next_bulk_page(Documents),
       Resp = insert_sync_page(Conn, Pool, Collection, Head, Tail, Options, undefined),
       case lists:member(response_options, Options) of
         true  -> Resp;
         false -> get_sync_result_new_proto(Resp, false)
       end;
     true ->
       Packet  = emongo_packet:insert(get_database(Pool, Collection), Collection,
                                      Pool#pool.req_id, Documents),
       sync_command(insert_sync, Collection, undefined, Options, Conn, Pool, Packet)
  end.

insert_sync_page(_, _, _, [], _, _, AccResp) ->
  AccResp;
insert_sync_page(Conn, Pool, Collection, Head, Tail, Options, AccResp) ->
  Ordered    = proplists:get_value(ordered, Options, true),
  InsertDoc  = [{<<"documents">>, {array, Head}},
                get_ordered_option(Ordered),
                get_writeconcern_option(Options, Pool)],
  Query      = create_cmd(<<"insert">>, Collection, InsertDoc, -1, Options),
  Packet     = emongo_packet:do_query(get_database(Pool, Collection), "$cmd", Pool#pool.req_id, Query),
  Resp       = send_recv_command(insert_sync, Collection, undefined, Options, Conn, Pool, Packet),
  NewAccResp = merge_response_docs(AccResp, Resp),

  % Break out early if the request is ordered and a write error occurred
  case Ordered andalso proplists:is_defined(<<"writeErrors">>, hd(Resp#response.documents)) of
    true ->
      NewAccResp;
    false ->
      {NewHead, NewTail} = next_bulk_page(Tail),
      insert_sync_page(Conn, Pool, Collection, NewHead, NewTail, Options, NewAccResp)
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
  {Conn, Pool} = gen_server:call(?MODULE, {conn, PoolId}, infinity),
  Query = create_query(Options, Selector),
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
  {Conn, Pool} = gen_server:call(?MODULE, {conn, PoolId}, infinity),
  Query = create_query(Options, Selector),
  Packet = emongo_packet:update(get_database(Pool, Collection), Collection,
                                Pool#pool.req_id, false, true, Query#emo_query.q,
                                Document),
  send_command(update_all, Collection, Selector, [], Conn, Pool, Packet).

%------------------------------------------------------------------------------
% update_sync that runs db.$cmd.findOne({getlasterror: 1});
% If no documents match the input Selector, {emongo_no_match_found, DbResponse}
% will be returned.
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
  {Conn, Pool} = gen_server:call(?MODULE, {conn, PoolId, 2}, infinity),
  Options1 = case Upsert of
    true -> Options;
    _    -> [check_match_found | Options]
  end,
  if ?IS_MONGO_2_6(Pool#pool.max_wire_version) ->
       UpdateDoc = [{<<"updates">>, [[{<<"q">>,      {struct, transform_selector(Selector)}},
                                      {<<"u">>,      {struct, Document}},
                                      {<<"upsert">>, Upsert}]]},
                    get_ordered_option(Options),
                    get_writeconcern_option(Options, Pool)],
       Query   = create_cmd(<<"update">>, Collection, UpdateDoc, -1, Options),
       Packet  = emongo_packet:do_query(get_database(Pool, Collection), "$cmd", Pool#pool.req_id, Query),
       Resp    = send_recv_command(update_sync, Collection, Selector, Options, Conn, Pool, Packet),
       case lists:member(response_options, Options) of
         true  -> Resp;
         false -> get_sync_result_new_proto(Resp, not Upsert)
       end;
     true ->
       Query = create_query(Options, Selector),
       Packet1 = emongo_packet:update(get_database(Pool, Collection), Collection,
                                      Pool#pool.req_id, Upsert, false, Query#emo_query.q,
                                      Document),
       sync_command(update_sync, Collection, Selector, Options1, Conn, Pool, Packet1)
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
  {Conn, Pool} = gen_server:call(?MODULE, {conn, PoolId, 2}, infinity),
  if ?IS_MONGO_2_6(Pool#pool.max_wire_version) ->
       UpdateDoc = [{<<"updates">>, [[{<<"q">>,     {struct, transform_selector(Selector)}},
                                      {<<"u">>,     {struct, Document}},
                                      {<<"multi">>, true}]]},
                    get_ordered_option(Options),
                    get_writeconcern_option(Options, Pool)],
       Query   = create_cmd(<<"update">>, Collection, UpdateDoc, -1, Options),
       Packet  = emongo_packet:do_query(get_database(Pool, Collection), "$cmd", Pool#pool.req_id, Query),
       Resp    = send_recv_command(update_all_sync, Collection, Selector, Options, Conn, Pool, Packet),
       case lists:member(response_options, Options) of
         true  -> Resp;
         false -> get_sync_result_new_proto(Resp, false)
       end;
     true ->
       Query = create_query(Options, Selector),
       Packet1 = emongo_packet:update(get_database(Pool, Collection), Collection,
                                      Pool#pool.req_id, false, true, Query#emo_query.q,
                                      Document),
       % We could check <<"n">> as the update_sync(...) functions do, but update_all_sync(...)
       % isn't targeting a specific number of documents, so 0 updates is legitimate.
       sync_command(update_all_sync, Collection, Selector, Options, Conn, Pool, Packet1)
  end.

%------------------------------------------------------------------------------
% delete
%------------------------------------------------------------------------------
delete(PoolId, Collection) ->
  delete(PoolId, Collection, []).

delete(PoolId, Collection, Selector) ->
  delete(PoolId, Collection, Selector, []).

delete(PoolId, Collection, Selector, Options) ->
  {Conn, Pool} = gen_server:call(?MODULE, {conn, PoolId}, infinity),
  Query = create_query(Options, Selector),
  Packet = emongo_packet:delete(get_database(Pool, Collection), Collection,
                                Pool#pool.req_id, Query#emo_query.q),
  send_command(delete, Collection, Selector, Options, Conn, Pool, Packet).

%------------------------------------------------------------------------------
% delete_sync that runs db.$cmd.findOne({getlasterror: 1});
% If no documents match the input Selector, {emongo_no_match_found, DbResponse}
% will be returned.
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
  {Conn, Pool} = gen_server:call(?MODULE, {conn, PoolId, 2}, infinity),
  if ?IS_MONGO_2_6(Pool#pool.max_wire_version) ->
       DeleteDoc = [{<<"deletes">>, [[{<<"q">>,     {struct, transform_selector(Selector)}},
                                      {<<"limit">>, 0}]]},
                    get_ordered_option(Options),
                    get_writeconcern_option(Options, Pool)],
       Query   = create_cmd(<<"delete">>, Collection, DeleteDoc, -1, Options),
       Packet  = emongo_packet:do_query(get_database(Pool, Collection), "$cmd", Pool#pool.req_id, Query),
       Resp    = send_recv_command(delete_sync, Collection, Selector, Options, Conn, Pool, Packet),
       case lists:member(response_options, Options) of
         true  -> Resp;
         false -> get_sync_result_new_proto(Resp, true)
       end;
     true ->
       Query = create_query(Options, Selector),
       Packet1 = emongo_packet:delete(get_database(Pool, Collection), Collection,
                                      Pool#pool.req_id,
                                      Query#emo_query.q),
       sync_command(delete_sync, Collection, Selector, [check_match_found | Options], Conn, Pool, Packet1)
  end.

%------------------------------------------------------------------------------
% ensure index (deprecated, use create_index instead)
%------------------------------------------------------------------------------
ensure_index(PoolId, Collection, Keys, Unique) when ?IS_DOCUMENT(Keys)->
  {Conn, Pool} = gen_server:call(?MODULE, {conn, PoolId}, infinity),
  % If mongo 2.6 or newer, use create_index instead
  if ?IS_MONGO_2_6(Pool#pool.max_wire_version) ->
       Options = case Unique of
         true -> [{<<"unique">>, true}];
         _    -> []
       end,
       create_index(Conn, Pool, Collection, Keys, undefined, Options);
     true ->
       Packet = emongo_packet:ensure_index(get_database(Pool, Collection), Collection,
                                           Pool#pool.req_id, Keys, Unique),
       send_command(ensure_index, Collection, Keys, [], Conn, Pool, Packet)
  end.

%------------------------------------------------------------------------------
% create index
%------------------------------------------------------------------------------
create_index(PoolId, Collection, Keys, Options) when ?IS_DOCUMENT(Keys)  ->
  create_index(PoolId, Collection, Keys, undefined, Options).

create_index(PoolId, Collection, Keys, IndexName, Options) when ?IS_DOCUMENT(Keys) ->
  {Conn, Pool} = gen_server:call(?MODULE, {conn, PoolId}, infinity),
  create_index(Conn, Pool, Collection, Keys, IndexName, Options).

create_index(Conn, Pool, Collection, Keys, IndexName, Options) ->
  Name = case IndexName of
    undefined -> generate_index_name(Keys, <<>>);
    _         -> IndexName
  end,
  Index        = [{<<"key">>, Keys}, {<<"name">>, Name}] ++ Options,
  CreateIdxDoc = [{<<"indexes">>, {array, [Index]}}],
  Query        = create_cmd(<<"createIndexes">>, Collection, CreateIdxDoc, -1, []),
  Packet       = emongo_packet:do_query(get_database(Pool, Collection), "$cmd", Pool#pool.req_id, Query),
  Resp         = send_recv_command(create_index, Collection, undefined, [], Conn, Pool, Packet),
  get_sync_result_new_proto(Resp, false).

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
  {Conn, Pool} = gen_server:call(?MODULE, {conn, PoolId}, infinity),
  DropIdxDoc   = [{<<"index">>, IndexName}],
  Query        = create_cmd(<<"dropIndexes">>, Collection, DropIdxDoc, -1, []),
  Packet       = emongo_packet:do_query(get_database(Pool, Collection), "$cmd", Pool#pool.req_id, Query),
  Resp         = send_recv_command(drop_index, Collection, undefined, [], Conn, Pool, Packet),
  get_sync_result_new_proto(Resp, false).

%------------------------------------------------------------------------------
% count
%------------------------------------------------------------------------------
count(PoolId, Collection) ->
  count(PoolId, Collection, [], []).

count(PoolId, Collection, Selector) ->
  count(PoolId, Collection, Selector, []).

count(PoolId, Collection, Selector, OptionsIn) ->
  Options      = set_read_preference(PoolId, OptionsIn),
  {Conn, Pool} = gen_server:call(?MODULE, {conn, PoolId}, infinity),
  CountDoc     = [{<<"query">>, {struct, transform_selector(Selector)}}],
  Query        = create_cmd(<<"count">>, Collection, CountDoc, 1, Options),
  Packet       = emongo_packet:do_query(get_database(Pool, Collection), "$cmd", Pool#pool.req_id, Query),
  Resp         = send_recv_command(count, Collection, Selector, Options, Conn, Pool, Packet),
  RespOpts     = lists:member(response_options, Options),
  case Resp of
    _ when RespOpts -> Resp;
    #response{documents=[Doc]} ->
      case proplists:get_value(<<"n">>, Doc, undefined) of
        undefined -> undefined;
        Count     -> round(Count)
      end;
    _ -> undefined
  end.

%%------------------------------------------------------------------------------
%% aggregate
%%------------------------------------------------------------------------------
aggregate(PoolId, Collection, Pipeline) ->
  aggregate(PoolId, Collection, Pipeline, []).

aggregate(PoolId, Collection, Pipeline, OptionsIn) ->
  Options      = set_read_preference(PoolId, OptionsIn),
  {Conn, Pool} = gen_server:call(?MODULE, {conn, PoolId}, infinity),
  AggregateDoc = [{<<"pipeline">>, {array, Pipeline}}],
  Query        = create_cmd(<<"aggregate">>, Collection, AggregateDoc, 1, Options),
  Packet       = emongo_packet:do_query(get_database(Pool, Collection), "$cmd", Pool#pool.req_id, Query),
  Resp         = send_recv_command(aggregate, Collection, Pipeline, Options, Conn, Pool, Packet),
  RespOpts     = lists:member(response_options, Options),
  case Resp of
    _ when RespOpts -> Resp;
    #response{documents=[Doc]} ->
      case proplists:get_value(<<"ok">>, Doc, undefined) of
        undefined -> throw({emongo_aggregation_failed, Resp});
        _ ->
          {array, Res} = proplists:get_value(<<"result">>, Doc),
          Res
      end
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
  {Conn, Pool} = gen_server:call(?MODULE, {conn, PoolId}, infinity),
  Timeout      = get_timeout(Options, Pool),
  FindModDoc   = [{<<"query">>, {struct, transform_selector(Selector)}},
                  {<<"update">>, Update},
                  {<<"maxTimeMS">>, Timeout}],
  WriteConcern = if ?IS_MONGO_3_2(Pool#pool.max_wire_version) -> [get_writeconcern_option(Options, Pool)];
                    true -> []
                 end,
  Query        = create_cmd(<<"findandmodify">>, Collection, FindModDoc ++ WriteConcern, undefined,
                            % We don't want to force the limit to 1, but want to default it to 1 if it's not in Options.
                            [{limit, 1} | Options]),
  Packet       = emongo_packet:do_query(get_database(Pool, Collection), "$cmd", Pool#pool.req_id, Query),
  Resp         = send_recv_command(find_and_modify, Collection, Selector, Options, Conn, Pool, Packet),
  case lists:member(response_options, Options) of
    true  -> Resp;
    false ->
    % TODO - throw error (specific error or a generic emongo_error) if errmsg is returned,
    % else return the documents. To do this we will need to make sure any application using
    % find_and_modify is able to handle the errors.
      #response{documents=[Res]} = Resp,
      case proplists:get_value(<<"errmsg">>, Res) of
        undefined     -> ok;
        ErrorMsg      -> throw_specific_errors(ErrorMsg, Res)
      end,
      Resp#response.documents
  end.

%====================================================================
% db collection operations
%====================================================================
drop_collection(PoolId, Collection) -> drop_collection(PoolId, Collection, []).

drop_collection(PoolId, Collection, Options) when is_atom(PoolId) ->
  {Conn, Pool} = gen_server:call(?MODULE, {conn, PoolId}, infinity),
  TQuery   = create_query([], [{<<"drop">>, Collection}]),
  Query    = TQuery#emo_query{limit=-1}, %dont ask me why, it just has to be -1
  Packet   = emongo_packet:do_query(get_database(Pool, Collection), "$cmd", Pool#pool.req_id, Query),
  Resp     = send_recv_command(drop_collection, "$cmd", Query, Options, Conn, Pool, Packet),
  RespOpts = lists:member(response_options, Options),
  case Resp of
    _ when RespOpts -> Resp;
    #response{documents=[Res]} ->
      case proplists:get_value(<<"ok">>, Res, undefined) of
        undefined ->
          case proplists:get_value(<<"errmsg">>, Res, undefined) of
            undefined -> throw({emongo_drop_collection_failed, proplists:get_value(<<"msg">>, Res, <<>>)});
            Error     -> throw({emongo_drop_collection_failed, Error})
          end;
        _ -> ok
      end
  end.

get_collections(PoolId) -> get_collections(PoolId, []).
get_collections(PoolId, OptionsIn) ->
  Options = set_read_preference(PoolId, OptionsIn),
  {Conn, Pool} = gen_server:call(?MODULE, {conn, PoolId}, infinity),
  Query    = create_query(Options, []),
  Database = to_binary(get_database(Pool, undefined)),
  Packet   = emongo_packet:do_query(Database, ?SYS_NAMESPACES, Pool#pool.req_id, Query),
  Resp     = send_recv_command(get_collections, ?SYS_NAMESPACES, Query, Options, Conn, Pool, Packet),
  RespOpts = lists:member(response_options, Options),
  case Resp of
    _ when RespOpts -> Resp;
    #response{documents=Docs} ->
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
  Options = set_read_preference(PoolId, OptionsIn),
  {Conn, Pool} = gen_server:call(?MODULE, {conn, PoolId}, infinity),

  TQuery   = create_query(Options, [{<<"listDatabases">>, 1}]),
  Query    = TQuery#emo_query{limit=-1}, %dont ask me why, it just has to be -1
  Database = to_binary(get_database(Pool, undefined)),
  Packet   = emongo_packet:do_query(Database, "$cmd", Pool#pool.req_id, Query),
  Resp     = send_recv_command(get_databases, "$cmd", Query, Options, Conn, Pool, Packet),

  RespOpts = lists:member(response_options, Options),
  case Resp of
    _ when RespOpts -> Resp;
    #response{documents=[Res]} ->
      case proplists:get_value(<<"ok">>, Res, undefined) of
        undefined ->
          case proplists:get_value(<<"errmsg">>, Res, undefined) of
            undefined -> throw({emongo_get_databases_failed, proplists:get_value(<<"msg">>, Res, <<>>)});
            Error     -> throw({emongo_get_databases_failed, Error})
          end;
        _ -> ok
      end
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
  {Conn, Pool} = gen_server:call(?MODULE, {conn, PoolId}, infinity),
  Query = #emo_query{q = Command, limit=-1},
  Packet = emongo_packet:do_query(get_database(Pool, undefined), "$cmd", Pool#pool.req_id, Query),
  Resp = send_recv_command(command, "$cmd", Command, Options, Conn, Pool, Packet),
  case lists:member(response_options, Options) of
    true -> Resp;
    false -> Resp#response.documents
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
  {Conn, Pool} = gen_server:call(?MODULE, {conn, PoolId}, infinity),
  Selector = [{<<"dropDatabase">>, 1}],
  TQuery   = create_query([], Selector),
  Query    = TQuery#emo_query{limit=-1}, %dont ask me why, it just has to be -1
  Packet   = emongo_packet:do_query(get_database(Pool, undefined), "$cmd", Pool#pool.req_id, Query),
  Resp     = send_recv_command(drop_database, undefined, Selector, Options, Conn, Pool, Packet),
  RespOpts = lists:member(response_options, Options),
  case Resp of
    _ when RespOpts -> Resp;
    #response{documents=[Res]} ->
      case proplists:get_value(<<"ok">>, Res, undefined) of
        undefined ->
          case proplists:get_value(<<"errmsg">>, Res, undefined) of
            undefined -> throw({emongo_drop_database_failed, proplists:get_value(<<"msg">>, Res, <<>>)});
            Error     -> throw({emongo_drop_database_failed, Error})
          end;
        _ -> ok
      end
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
  ets:new(?EMONGO_CONFIG, [public, named_table, {read_concurrency, true}]),
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
                          max_pipeline_depth  = MaxPipelineDepth,
                          socket_options      = SocketOptions,
                          conns               = Conns,
                          disconnect_timeouts = DisconnectTimeouts} = Pool) ->
  case queue:len(Conns) < Size of
    true ->
      % The emongo_conn:start_link function will throw an exception if it is unable to connect.
      {ok, Conn} = emongo_conn:start_link(PoolId, Host, Port, MaxPipelineDepth, DisconnectTimeouts, SocketOptions),
      MaxWireVer = case Pool#pool.max_wire_version of
        undefined -> get_max_wire_version(<<"admin">>, Pool, Conn);
        Ver       -> Ver
      end,
      NewPool = do_auth(Conn, Pool#pool{max_wire_version = MaxWireVer}),
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
do_auth(Conn, Pool) ->
  % Get all coll_to_db maps where AuthFlag is true
  CollToDbs = [DB || [DB, true] <- ets:match(?EMONGO_CONFIG, {{coll_to_db, Pool#pool.id, '_'}, '$1', '$2'})],
  RegisteredDBs = [Pool#pool.database | CollToDbs],
  UniqueDBs = lists:usort(RegisteredDBs),
  do_auth(UniqueDBs, Conn, Pool).

do_auth([], _Conn, Pool) -> Pool;
do_auth([DB | Others], Conn, #pool{user = User, pass_hash = PassHash} = Pool) when ?IS_MONGO_2_6(Pool#pool.max_wire_version) ->
  Bytes = crypto:rand_bytes(6),
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
  do_auth(Others, Conn, Pool#pool{req_id = Pool#pool.req_id + 1});

do_auth([DB | Others], Conn, #pool{user = User, pass_hash = PassHash} = Pool) ->
  Nonce = case getnonce(Conn, DB, Pool) of
    error -> throw(emongo_getnonce);
    N     -> N
  end,
  Digest = emongo:dec2hex(erlang:md5(<<Nonce/binary, User/binary, PassHash/binary>>)),
  Query = #emo_query{q=[{<<"authenticate">>, 1}, {<<"user">>, User},
                        {<<"nonce">>, Nonce}, {<<"key">>, Digest}], limit=1},
  authorize_conn_for_db(DB, Pool, Query, Conn),
  do_auth(Others, Conn, Pool#pool{req_id = Pool#pool.req_id + 1}).

get_max_wire_version(DB, Pool, Conn) ->
  IsMasterQuery = #emo_query{q = [{<<"isMaster">>, 1}], limit = 1},
  Packet = emongo_packet:do_query(DB, "$cmd", Pool#pool.req_id, IsMasterQuery),
  Resp = send_recv_command(do_auth, "$cmd", undefined, [], Conn, Pool, Packet),
  [Res] = Resp#response.documents,
  case proplists:get_value(<<"ok">>, Res) of
    1.0 -> case proplists:get_value(<<"maxWireVersion">>, Res) of
             undefined -> 0;
             Version   -> Version
           end;
    _   -> 0
  end.

authorize_conn_for_db(DB, Pool, Query, Conn) ->
  Packet = emongo_packet:do_query(DB, "$cmd", Pool#pool.req_id, Query),
  Resp = send_recv_command(do_auth, "$cmd", undefined, [], Conn, Pool, Packet),
  [Res] = Resp#response.documents,
  case proplists:get_value(<<"ok">>, Res) of
    1.0 -> ok;
    _   -> throw({emongo_authentication_failed, proplists:get_value(<<"errmsg">>, Res)})
  end.

scram_authorize_conn(DB, Pool, Query, Conn) ->
  Packet = emongo_packet:do_query(DB, "$cmd", Pool#pool.req_id, Query),
  Resp = send_recv_command(scram_authorize_conn, "$cmd", undefined, [], Conn, Pool, Packet),
  [Res] = Resp#response.documents,
  case proplists:get_value(<<"ok">>, Res) of
    1.0 -> EncodedPayload = proplists:get_value(<<"payload">>, Res),
           Payload = parse_scram_response(base64:decode(EncodedPayload)),
           {Res, Payload};
    _   -> throw({emongo_authentication_failed, proplists:get_value(<<"errmsg">>, Resp)})
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

getnonce(Conn, DB, Pool) ->
  Query1 = #emo_query{q=[{<<"getnonce">>, 1}], limit=1},
  Packet = emongo_packet:do_query(DB, "$cmd", Pool#pool.req_id, Query1),
  Resp1 = send_recv_command(getnonce, "$cmd", undefined, [], Conn, Pool, Packet),
  case proplists:get_value(<<"nonce">>, lists:nth(1, Resp1#response.documents), undefined) of
    undefined -> error;
    Nonce     -> Nonce
  end.

get_pool(PoolId, Pools) ->
  get_pool(PoolId, Pools, []).

get_pool(_, [], _) ->
  undefined;

get_pool(PoolId, [{PoolId, Pool}|Tail], Others) ->
  {Pool, lists:append(Tail, Others)};

get_pool(PoolId, [Pool|Tail], Others) ->
  get_pool(PoolId, Tail, [Pool|Others]).

get_database(Pool, Collection) ->
  case ets:lookup(?EMONGO_CONFIG, {coll_to_db, Pool#pool.id, to_binary(Collection)}) of
    [{_, Database, true}]  -> Database;
    [{_, Database, false}] ->
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
    when Ignore == check_match_found;
         Ignore == response_options ->
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

sync_command(Command, Collection, Selector, Options, Conn, Pool, Packet1) ->
  % When this connection was requested, two request ids were allocated, so using req_id + 1 is safe.
  GetLastErrorReqId   = Pool#pool.req_id + 1,
  Timeout             = get_timeout(Options, Pool),
  WriteConcern        = proplists:get_value(write_concern,         Options, Pool#pool.write_concern),
  WriteConcernTimeout = proplists:get_value(write_concern_timeout, Options, Pool#pool.write_concern_timeout),
  Query1 = #emo_query{q = [{<<"getlasterror">>, 1}, {<<"w">>, WriteConcern}, {<<"wtimeout">>, WriteConcernTimeout}],
                      limit = 1},
  Packet2 = emongo_packet:do_query(get_database(Pool, Collection), "$cmd", GetLastErrorReqId, Query1),
  try
    Resp = time_call({Command, Collection, Selector, Options}, fun() ->
      emongo_conn:send_sync(Conn, GetLastErrorReqId, Packet1, Packet2, Timeout)
    end),
    case lists:member(response_options, Options) of
      true  -> Resp;
      false -> get_sync_result_old_proto(Resp, lists:member(check_match_found, Options))
    end
  catch _:{emongo_conn_error, Error} ->
    throw({emongo_conn_error, Error, Command, Collection, Selector,
           [{options, Options}, {msg_queue_len, emongo_conn:queue_lengths(Conn)}]})
  end.

send_recv_command(Command, Collection, Selector, Options, Conn, Pool, Packet) ->
  try
    time_call({Command, Collection, Selector, Options}, fun() ->
      emongo_conn:send_recv(Conn, Pool#pool.req_id, Packet, get_timeout(Options, Pool))
    end)
  catch _:{emongo_conn_error, Error} ->
    throw({emongo_conn_error, Error, Command, Collection, Selector,
           [{options, Options}, {msg_queue_len, emongo_conn:queue_lengths(Conn)}]})
  end.

send_command(Command, Collection, Selector, Options, Conn, Pool, Packet) ->
  try
    time_call({Command, Collection, Selector, Options}, fun() ->
      emongo_conn:send(Conn, Pool#pool.req_id, Packet, get_timeout(Options, Pool))
    end)
  catch _:{emongo_conn_error, Error} ->
    throw({emongo_conn_error, Error, Command, Collection, Selector,
           [{options, Options}, {msg_queue_len, emongo_conn:queue_lengths(Conn)}]})
  end.

% TODO: Include selector in emongo_error messages.

get_sync_result_old_proto(#response{documents = [Doc]}, CheckMatchFound) ->
  case proplists:get_value(<<"err">>, Doc, undefined) of
    undefined     -> check_match_found(Doc, CheckMatchFound);
    <<"timeout">> -> throw({emongo_conn_error, db_timeout});
    ErrorMsg      -> throw_specific_errors(ErrorMsg, Doc),
                     throw({emongo_error, ErrorMsg})
  end;
get_sync_result_old_proto(Resp, _CheckMatchFound) ->
  throw({emongo_error, {invalid_response, Resp}}).

get_sync_result_new_proto(Resp = #response{documents = [Doc]}, CheckMatchFound) ->
  % Throw an exception if there were write errors returned
  case proplists:get_value(<<"writeErrors">>, Doc, undefined) of
    undefined -> ok;
    {array, ErrorList} ->
      lists:foreach(fun(Error) ->
        ErrorMsg = proplists:get_value(<<"errmsg">>, Error, undefined),
        throw_specific_errors(ErrorMsg, Error)
      end, ErrorList),
      throw({emongo_error, ErrorList});
    _ -> ok
  end,

  % Throw an exception if there were write concern errors returned
  case proplists:get_value(<<"writeConcernError">>, Doc, undefined) of
    undefined -> ok;
    Error     -> throw({emongo_error, Error})
  end,

  % In some cases, error can be under <<"errmsg">> as well
  case proplists:get_value(<<"errmsg">>, Doc, undefined) of
    undefined     -> ok;
    ErrorMsg      -> throw_specific_errors(ErrorMsg, Doc),
                     throw({emongo_error, ErrorMsg})
  end,

  % Check the value of the ok
  case proplists:get_value(<<"ok">>, Doc, undefined) of
    undefined -> throw({emongo_error, {invalid_response, Resp}});
    _         -> ok
  end,

  % Check the value of n, if CheckMatchFound is true
  case CheckMatchFound andalso proplists:get_value(<<"n">>, Doc, undefined) of
    undefined -> throw({emongo_error, {invalid_response, Resp}});
    0         -> {emongo_no_match_found, Doc};
    _         -> ok
  end;
get_sync_result_new_proto(Resp, _CheckMatchFound) ->
  throw({emongo_error, {invalid_response, Resp}}).

check_match_found(_Doc, false) -> ok;
check_match_found(Doc, _) ->
  % For some reason, "updatedExisting" isn't always in the response.  If it is there, use the value it returns.
  % Otherwise, fall back on "n", which seems to always be there.
  case proplists:get_value(<<"updatedExisting">>, Doc) of
    true  -> ok;
    false -> {emongo_no_match_found, Doc};
    _ ->
      case proplists:get_value(<<"n">>, Doc, undefined) of
        undefined -> throw({emongo_error, {invalid_response, Doc}});
        0         -> {emongo_no_match_found, Doc};
        _         -> ok
      end
  end.

throw_specific_errors(ErrorMsg, Doc) ->
  Code = proplists:get_value(<<"code">>, Doc),
  if
    Code == 11000 orelse Code == 11001 -> throw({emongo_error, duplicate_key, ErrorMsg});
    Code == 13                         -> throw({emongo_authorization_error, ErrorMsg});
    true                               -> ok
  end.

time_call({Command, Collection, Selector, _Options}, Fun) ->
  {TimeUsec, Res} = timer:tc(fun() ->
    try
      Fun()
    catch C:E ->
      {exception, C, E, erlang:get_stacktrace()}
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

get_timeout(Options, Pool) ->
  proplists:get_value(timeout, Options, Pool#pool.timeout).

get_ordered_option(Ordered) when is_boolean(Ordered) ->
  {<<"ordered">>, Ordered};
get_ordered_option(Options) ->
  {<<"ordered">>, proplists:get_value(ordered, Options, true)}.

get_writeconcern_option(Options, Pool) ->
  {<<"writeConcern">>, {struct,
    [{<<"w">>,        proplists:get_value(write_concern,         Options, Pool#pool.write_concern)},
     {<<"j">>,        proplists:get_value(journal_write_ack,     Options, Pool#pool.journal_write_ack)},
     {<<"wtimeout">>, proplists:get_value(write_concern_timeout, Options, Pool#pool.write_concern_timeout)}]}}.

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
  DefaultReadPref = ets:lookup_element(?EMONGO_CONFIG, {default_read_pref, PoolId}, 2),
  ReadPref        = proplists:get_value(read_pref, Options, DefaultReadPref),
  set_read_preference_int(to_binary(ReadPref), proplists:delete(read_pref, Options)).

set_read_preference_int(undefined, Options) -> Options;
set_read_preference_int(ReadPref,  Options) ->
  [{<<"$readPreference">>, [{<<"mode">>, ReadPref}]} | Options].

% Merge response documents when calling a bulk function in batches
merge_response_docs(undefined, Resp = #response{documents = [_Doc]}) ->
  Resp;
merge_response_docs(AccResp = #response{documents = [AccDoc]}, #response{documents = [Doc]}) ->
  NewDoc =
    [
      {<<"ok">>, proplists:get_value(<<"ok">>, AccDoc, 0) band proplists:get_value(<<"ok">>, Doc, 0)},
      {<<"n">>,  proplists:get_value(<<"n">>, AccDoc, 0)  +    proplists:get_value(<<"n">>, Doc, 0)}
    ] ++
    combine_write_errors(<<"writeErrors">>, AccDoc, Doc) ++
    combine_write_errors(<<"writeConcernError">>, AccDoc, Doc),
  AccResp#response{documents = [NewDoc]};
merge_response_docs(_, Resp) ->
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
next_bulk_page([]) ->
  {[], []};
next_bulk_page(Documents) when length(Documents) > ?WRITE_CMD_LIMIT ->
  lists:split(?WRITE_CMD_LIMIT, Documents);
next_bulk_page(Documents) ->
  {Documents, []}.

% c("../../deps/emongo/src/emongo.erl", [{i, "../../deps/emongo/include"}]).
