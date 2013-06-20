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

-export([start_link/0, init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

-export([pools/0, oid/0, oid_generation_time/1, add_pool/5, add_pool/7,
         remove_pool/1, find/4, find_all/2, find_all/3, find_all/4,
         get_more/4, get_more/5, find_one/3, find_one/4, kill_cursors/2,
         insert/3, insert_sync/3, insert_sync/4, update/4, update/5,
         update_all/4, update_sync/4, update_sync/5, update_sync/6,
         update_all_sync/4, update_all_sync/5, delete/2, delete/3,
         delete_sync/2, delete_sync/3, delete_sync/4, ensure_index/4, count/2,
         aggregate/3, aggregate/4, get_collections/1, get_collections/2,
         count/3, count/4, find_and_modify/4, find_and_modify/5, dec2hex/1,
         hex2dec/1, utf8_encode/1, drop_collection/2, drop_collection/3,
         drop_database/1, drop_database/2]).

-include("emongo.hrl").
-include_lib("eunit/include/eunit.hrl").
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
  gen_server:call(?MODULE, oid, infinity).

oid_generation_time({oid, Oid}) ->
  oid_generation_time(Oid);
oid_generation_time(Oid) when is_binary(Oid) andalso size(Oid) =:= 12 ->
  <<UnixTime:32/signed, _/binary>> = Oid,
  UnixTime.

add_pool(PoolId, Host, Port, Database, Size) ->
  gen_server:call(?MODULE, {add_pool, PoolId, Host, Port, Database, Size,
                            undefined, undefined}, infinity).

add_pool(PoolId, Host, Port, Database, Size, User, Pass) ->
  gen_server:call(?MODULE, {add_pool, PoolId, Host, Port, Database, Size,
                            User, pass_hash(User, Pass)}, infinity).

remove_pool(PoolId) ->
  gen_server:call(?MODULE, {remove_pool, PoolId}).

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
find(PoolId, Collection, Selector, OptionsIn) when ?IS_DOCUMENT(Selector),
                                                   is_list(OptionsIn) ->
  {Pid, Pool} = gen_server:call(?MODULE, {pid, PoolId}, infinity),
  Options = set_slave_ok(OptionsIn),
  Query = create_query(Options, Selector),
  Packet = emongo_packet:do_query(Pool#pool.database, Collection,
                                  Pool#pool.req_id, Query),
  Resp = send_recv_command(find, Collection, Selector, Options, Pid,
                           Pool#pool.req_id, Packet,
                           proplists:get_value(timeout, Options, ?TIMEOUT)),
  case lists:member(response_options, Options) of
    true -> Resp;
    false -> Resp#response.documents
  end.

%------------------------------------------------------------------------------
% find_all
%------------------------------------------------------------------------------
find_all(PoolId, Collection) ->
  find_all(PoolId, Collection, [], [{timeout, ?TIMEOUT}]).

find_all(PoolId, Collection, Selector) when ?IS_DOCUMENT(Selector) ->
  find_all(PoolId, Collection, Selector, [{timeout, ?TIMEOUT}]).

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
  Resp1 = get_more(PoolId, Collection, Resp#response.cursor_id, proplists:get_value(timeout, Options, ?TIMEOUT)),
  Documents = lists:append(Resp#response.documents, Resp1#response.documents),
  find_all(PoolId, Collection, Selector, Options, Resp1#response{documents=Documents}).

%------------------------------------------------------------------------------
% find_one
%------------------------------------------------------------------------------
find_one(PoolId, Collection, Selector) when ?IS_DOCUMENT(Selector) ->
  find_one(PoolId, Collection, Selector, [{timeout, ?TIMEOUT}]).

find_one(PoolId, Collection, Selector, Options) when ?IS_DOCUMENT(Selector),
                                                     is_list(Options) ->
  Options1 = [{limit, 1} | lists:keydelete(limit, 1, Options)],
  find(PoolId, Collection, Selector, Options1).

%------------------------------------------------------------------------------
% get_more
%------------------------------------------------------------------------------
get_more(PoolId, Collection, CursorID, Timeout) ->
  get_more(PoolId, Collection, CursorID, 0, Timeout).

get_more(PoolId, Collection, CursorID, NumToReturn, Timeout) ->
  {Pid, Pool} = gen_server:call(?MODULE, {pid, PoolId}, infinity),
  Packet = emongo_packet:get_more(Pool#pool.database, Collection,
                                  Pool#pool.req_id, NumToReturn, CursorID),
  send_recv_command(get_more, Collection, CursorID,
                    {num_to_return, NumToReturn}, Pid, Pool#pool.req_id, Packet,
                    Timeout).

%------------------------------------------------------------------------------
% kill_cursors
%------------------------------------------------------------------------------
kill_cursors(PoolId, CursorID) when is_integer(CursorID) ->
  kill_cursors(PoolId, [CursorID]);

kill_cursors(PoolId, CursorIDs) when is_list(CursorIDs) ->
  {Pid, Pool} = gen_server:call(?MODULE, {pid, PoolId}, infinity),
  Packet = emongo_packet:kill_cursors(Pool#pool.req_id, CursorIDs),
  send_command(kill_cursors, undefined, CursorIDs, undefined, Pid,
               Pool#pool.req_id, Packet).

%------------------------------------------------------------------------------
% insert
%------------------------------------------------------------------------------
insert(PoolId, Collection, Document) when ?IS_DOCUMENT(Document) ->
  insert(PoolId, Collection, [Document]);

insert(PoolId, Collection, Documents) when ?IS_LIST_OF_DOCUMENTS(Documents) ->
  {Pid, Pool} = gen_server:call(?MODULE, {pid, PoolId}, infinity),
  Packet = emongo_packet:insert(Pool#pool.database, Collection,
                                Pool#pool.req_id, Documents),
  send_command(insert, Collection, undefined, undefined, Pid, Pool#pool.req_id,
               Packet).

%------------------------------------------------------------------------------
% insert_sync that runs db.$cmd.findOne({getlasterror: 1});
%------------------------------------------------------------------------------
insert_sync(PoolId, Collection, Documents) ->
  insert_sync(PoolId, Collection, Documents, []).

insert_sync(PoolId, Collection, DocumentsIn, Options) ->
  Documents = if
    ?IS_LIST_OF_DOCUMENTS(DocumentsIn) -> DocumentsIn;
    ?IS_DOCUMENT(DocumentsIn)      -> [DocumentsIn]
  end,

  {Pid, Pool} = gen_server:call(?MODULE, {pid, PoolId}, infinity),
  Packet1 = emongo_packet:insert(Pool#pool.database, Collection,
                                 Pool#pool.req_id, Documents),
  sync_command(insert_sync, Collection, undefined, Options, {Pid, Pool},
               Packet1).

%------------------------------------------------------------------------------
% update
%------------------------------------------------------------------------------
update(PoolId, Collection, Selector, Document) when ?IS_DOCUMENT(Selector),
                                                    ?IS_DOCUMENT(Document) ->
  update(PoolId, Collection, Selector, Document, false).

update(PoolId, Collection, Selector, Document, Upsert)
    when ?IS_DOCUMENT(Selector), ?IS_DOCUMENT(Document) ->
  {Pid, Pool} = gen_server:call(?MODULE, {pid, PoolId}, infinity),
  Packet = emongo_packet:update(Pool#pool.database, Collection,
                                Pool#pool.req_id, Upsert, false, Selector,
                                Document),
  send_command(update, Collection, Selector, {upsert, Upsert}, Pid,
               Pool#pool.req_id, Packet).

%------------------------------------------------------------------------------
% update_all
%------------------------------------------------------------------------------
update_all(PoolId, Collection, Selector, Document)
    when ?IS_DOCUMENT(Selector), ?IS_DOCUMENT(Document) ->
  {Pid, Pool} = gen_server:call(?MODULE, {pid, PoolId}, infinity),
  Packet = emongo_packet:update(Pool#pool.database, Collection,
                                Pool#pool.req_id, false, true, Selector,
                                Document),
  send_command(update_all, Collection, Selector, undefined, Pid,
               Pool#pool.req_id, Packet).

%------------------------------------------------------------------------------
% update_sync that runs db.$cmd.findOne({getlasterror: 1});
% If no documents match the input Selector, {emongo_no_match_found, DbResponse}
% will be returned.
%------------------------------------------------------------------------------
update_sync(PoolId, Collection, Selector, Document)
    when ?IS_DOCUMENT(Selector), ?IS_DOCUMENT(Document) ->
  update_sync(PoolId, Collection, Selector, Document, false).

update_sync(PoolId, Collection, Selector, Document, Upsert)
    when ?IS_DOCUMENT(Selector), ?IS_DOCUMENT(Document) ->
  update_sync(PoolId, Collection, Selector, Document, Upsert, []).

update_sync(PoolId, Collection, Selector, Document, Upsert, Options)
    when ?IS_DOCUMENT(Selector), ?IS_DOCUMENT(Document) ->
  {Pid, Pool} = gen_server:call(?MODULE, {pid, PoolId}, infinity),
  Packet1 = emongo_packet:update(Pool#pool.database, Collection,
                                 Pool#pool.req_id, Upsert, false, Selector,
                                 Document),
  Options1 = case Upsert of
    true -> Options;
    _    -> [check_match_found | Options]
  end,
  sync_command(update_sync, Collection, Selector, Options1, {Pid, Pool},
               Packet1).

%------------------------------------------------------------------------------
% update_all_sync that runs db.$cmd.findOne({getlasterror: 1});
%------------------------------------------------------------------------------
update_all_sync(PoolId, Collection, Selector, Document)
    when ?IS_DOCUMENT(Selector), ?IS_DOCUMENT(Document) ->
  update_all_sync(PoolId, Collection, Selector, Document, []).

update_all_sync(PoolId, Collection, Selector, Document, Options)
    when ?IS_DOCUMENT(Selector), ?IS_DOCUMENT(Document) ->
  {Pid, Pool} = gen_server:call(?MODULE, {pid, PoolId}, infinity),
  Packet1 = emongo_packet:update(Pool#pool.database, Collection,
                                 Pool#pool.req_id, false, true, Selector,
                                 Document),
  % We could check <<"n">> as the update_sync(...) functions do, but
  % update_all_sync(...) isn't targeting a specific number of documents, so 0
  % updates is legitimate.
  sync_command(update_all_sync, Collection, Selector, Options, {Pid, Pool},
               Packet1).

%------------------------------------------------------------------------------
% delete
%------------------------------------------------------------------------------
delete(PoolId, Collection) ->
  delete(PoolId, Collection, []).

delete(PoolId, Collection, Selector) ->
  {Pid, Pool} = gen_server:call(?MODULE, {pid, PoolId}, infinity),
  Packet = emongo_packet:delete(Pool#pool.database, Collection,
                                Pool#pool.req_id, transform_selector(Selector)),
  send_command(delete, Collection, Selector, undefined, Pid, Pool#pool.req_id,
               Packet).

%------------------------------------------------------------------------------
% delete_sync that runs db.$cmd.findOne({getlasterror: 1});
% If no documents match the input Selector, {emongo_no_match_found, DbResponse}
% will be returned.
%------------------------------------------------------------------------------
delete_sync(PoolId, Collection) ->
  delete_sync(PoolId, Collection, []).

delete_sync(PoolId, Collection, Selector) ->
  delete_sync(PoolId, Collection, Selector, []).

delete_sync(PoolId, Collection, Selector, Options) ->
  {Pid, Pool} = gen_server:call(?MODULE, {pid, PoolId}, infinity),
  Packet1 = emongo_packet:delete(Pool#pool.database, Collection,
                                 Pool#pool.req_id,
                                 transform_selector(Selector)),
  sync_command(delete_sync, Collection, Selector, [check_match_found | Options],
               {Pid, Pool}, Packet1).

%------------------------------------------------------------------------------
% ensure index
%------------------------------------------------------------------------------
ensure_index(PoolId, Collection, Keys, Unique) when ?IS_DOCUMENT(Keys)->
  {Pid, Pool} = gen_server:call(?MODULE, {pid, PoolId}, infinity),
  Packet = emongo_packet:ensure_index(Pool#pool.database, Collection,
                                      Pool#pool.req_id, Keys, Unique),
  send_command(ensure_index, Collection, Keys, undefined, Pid, Pool#pool.req_id,
               Packet).

%------------------------------------------------------------------------------
% count
%------------------------------------------------------------------------------
count(PoolId, Collection) ->
  count(PoolId, Collection, [], []).

count(PoolId, Collection, Selector) ->
  count(PoolId, Collection, Selector, []).

count(PoolId, Collection, Selector, OptionsIn) ->
  Options = set_slave_ok(OptionsIn),
  {Pid, Pool} = gen_server:call(?MODULE, {pid, PoolId}, infinity),
  Query = create_query([{<<"count">>, Collection}, {limit, 1} | Options],
                       Selector),
  Packet = emongo_packet:do_query(Pool#pool.database, "$cmd", Pool#pool.req_id,
                                Query),
  case send_recv_command(count, Collection, Selector, Options, Pid,
                         Pool#pool.req_id, Packet, ?TIMEOUT) of
    #response{documents=[Doc]} ->
      case proplists:get_value(<<"n">>, Doc, undefined) of
        undefined -> undefined;
        Count     -> round(Count)
      end;
    _ ->
      undefined
  end.

%%------------------------------------------------------------------------------
%% aggregate
%%------------------------------------------------------------------------------
aggregate(PoolId, Collection, Pipeline) ->
  aggregate(PoolId, Collection, Pipeline, []).

aggregate(PoolId, Collection, Pipeline, OptionsIn) ->
  Options = set_slave_ok(OptionsIn),
  {Pid, Pool} = gen_server:call(?MODULE, {pid, PoolId}, infinity),
  Query = create_query([{<<"pipeline">>, {array, Pipeline } },{<<"aggregate">>, Collection},{limit,1} | Options], []),
  Packet = emongo_packet:do_query(Pool#pool.database, "$cmd", Pool#pool.req_id, Query),
  case send_recv_command(aggregate, Collection, Pipeline, Options, Pid,
                         Pool#pool.req_id, Packet, ?TIMEOUT) of
    #response{documents=[Doc]} ->
      case proplists:get_value(<<"ok">>, Doc, undefined) of
        undefined -> undefined;
        _     -> Doc
      end;
      _ -> undefined
  end.

%%------------------------------------------------------------------------------
%% find_and_modify
%%------------------------------------------------------------------------------
find_and_modify(PoolId, Collection, Selector, Update) ->
  find_and_modify(PoolId, Collection, Selector, Update, []).

find_and_modify(PoolId, Collection, Selector, Update, Options)
  when ?IS_DOCUMENT(Selector), ?IS_DOCUMENT(Update), is_list(Options) ->
  {Pid, Pool} = gen_server:call(?MODULE, {pid, PoolId}, infinity),
  Collection1 = to_binary(Collection),
  Selector1 = transform_selector(Selector),
  Fields = proplists:get_value(fields, Options, []),
  FieldSelector = convert_fields(Fields),
  Options1 = proplists:delete(fields, Options),
  Options2 = [{to_binary(Opt), Val} || {Opt, Val} <- Options1],
  Query = #emo_query{q = [{<<"findandmodify">>, Collection1},
              {<<"query">>,     Selector1},
              {<<"update">>,    Update},
              {<<"fields">>,    FieldSelector}
              | Options2],
             limit = 1},
  Packet = emongo_packet:do_query(Pool#pool.database, "$cmd", Pool#pool.req_id,
                  Query),
  Resp = send_recv_command(find_and_modify, Collection, Selector, Options, Pid,
                           Pool#pool.req_id, Packet,
                           proplists:get_value(timeout, Options, ?TIMEOUT)),
  case lists:member(response_options, Options) of
    true -> Resp;
    false -> Resp#response.documents
  end.

%====================================================================
% db collection operations
%====================================================================
drop_collection(PoolId, Collection) -> drop_collection(PoolId, Collection, []).

drop_collection(PoolId, Collection, Options) when is_atom(PoolId) ->
  {Pid, Pool} = gen_server:call(?MODULE, {pid, PoolId}, infinity),
  TQuery = create_query([], [{<<"drop">>, Collection}]),
  Query = TQuery#emo_query{limit=-1}, %dont ask me why, it just has to be -1
  Packet = emongo_packet:do_query(Pool#pool.database, "$cmd", Pool#pool.req_id, Query),
  case send_recv_command(drop_collection, "$cmd", Query, [], Pid, Pool#pool.req_id, Packet, proplists:get_value(timeout, Options, ?TIMEOUT)) of
    #response{documents=[Res]} ->
        case lists:keyfind(<<"ok">>, 1, Res) of
            {<<"ok">>, 1.0} -> ok;
            _ ->
                case lists:keyfind(<<"errmsg">>, 1, Res) of
                    {<<"errmsg">>, Error} -> throw({drop_collection_failed, Error});
                    _ -> throw({drop_collection_failed, lists:keyfind(<<"msg">>, 1, Res)})
                end
        end;
    _ -> {drop_collection_failed, "oh snap, i dont know what happened"}
  end.

get_collections(PoolId) -> get_collections(PoolId, []).
get_collections(PoolId, Options) when is_atom(PoolId) ->
  {Pid, Pool} = gen_server:call(?MODULE, {pid, PoolId}, infinity),
  Query = create_query(Options, []),
  Packet = emongo_packet:do_query(Pool#pool.database, ?SYS_NAMESPACES, Pool#pool.req_id, Query),
  case send_recv_command(get_collections, ?SYS_NAMESPACES, Query, Options, Pid, Pool#pool.req_id, Packet,  proplists:get_value(timeout, Options, ?TIMEOUT)) of
    #response{documents=Docs} ->
        Database = list_to_binary(Pool#pool.database ++ "."),
        lists:foldl(fun(Doc, Accum) ->
            Collection = proplists:get_value(<<"name">>, Doc),
            case binary:match(Collection, <<".$_">>) of
                nomatch ->
                    [_Junk, RealName] = binary:split(Collection, Database),
                    [ RealName | Accum ];
                _ -> Accum
            end
        end, [], Docs);
    _ -> undefined
  end.

%====================================================================
% db holy cow operations
%====================================================================
%this is slightly ghetto fabulous, it just provides a very quick method to
%truncate the entire db and trash all of the collections
drop_database(PoolId) -> drop_database(PoolId, []).
drop_database(PoolId, Options) ->
    %doing this with emongo_conn:send_recv that way we do not get the last error from the
    %dropped db
  {Pid, Pool} = gen_server:call(?MODULE, {pid, PoolId}, infinity),
  Selector = [{<<"dropDatabase">>, 1}],
  TQuery = create_query([], Selector),
  Query = TQuery#emo_query{limit=-1}, %dont ask me why, it just has to be -1
  DropPacket = emongo_packet:do_query(Pool#pool.database, "$cmd", Pool#pool.req_id, Query),
  try
    case emongo_conn:send_recv(Pid, Pool#pool.req_id, DropPacket,  proplists:get_value(timeout, Options, ?TIMEOUT)) of
        #response{documents=[Res]} ->
            case lists:keyfind(<<"ok">>, 1, Res) of
                {<<"ok">>, 1.0} -> ok;
                _ ->
                    case lists:keyfind(<<"errmsg">>, 1, Res) of
                        {<<"errmsg">>, Err} -> throw({drop_database_failed, Err});
                        _ -> throw({drop_database_failed, lists:keyfind(<<"msg">>, 1, Res)})
                    end
            end;
        _ -> {drop_collection_failed, "oh snap, i dont know what happened"}
    end
  catch _:{emongo_conn_error, Error} ->
    throw({emongo_conn_error, Error, "$cmd", "undefined", Selector, Options})
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

handle_call(oid, _From, State) ->
  {MegaSecs, Secs, _} = now(),
  UnixTime = MegaSecs * 1000000 + Secs,
  <<_:20/binary,PID:2/binary,_/binary>> = term_to_binary(self()),
  Index = State#state.oid_index rem 16#ffffff,
  {reply, <<UnixTime:32/signed, (State#state.hashed_hostn)/binary, PID/binary,
            Index:24>>, State#state{oid_index = State#state.oid_index + 1}};

handle_call({add_pool, PoolId, Host, Port, Database, Size, User, PassHash},
            _From, #state{pools=Pools}=State) ->
  {Result, Pools1} =
    case proplists:is_defined(PoolId, Pools) of
      true ->
        Pool = proplists:get_value(PoolId, Pools),
        Pool1 = do_open_connections(Pool),
        {ok, [{PoolId, Pool1} | proplists:delete(PoolId, Pools)]};
      false ->
        Pool = #pool{id        = PoolId,
                     host      = Host,
                     port      = Port,
                     database  = Database,
                     size      = Size,
                     user      = User,
                     pass_hash = PassHash},
        Pool1 = do_open_connections(Pool),
        {ok, [{PoolId, Pool1} | Pools]}
    end,
  {reply, Result, State#state{pools=Pools1}};

handle_call({remove_pool, PoolId}, _From, #state{pools=Pools}=State) ->
  {Result, Pools1} =
    case proplists:get_value(PoolId, Pools) of
      undefined ->
        {not_found, Pools};
      #pool{conn_pids = ConnPids} ->
        lists:foreach(fun(Pid) ->
          emongo_conn:stop(Pid)
        end, queue:to_list(ConnPids)),
        {ok, lists:keydelete(PoolId, 1, Pools)}
    end,
  {reply, Result, State#state{pools=Pools1}};

handle_call({pid, PoolId}, _From, #state{pools=Pools}=State) ->
  case get_pool(PoolId, Pools) of
    undefined ->
      {reply, {undefined, undefined}, State};
    {Pool, Others} ->
      case queue:out(Pool#pool.conn_pids) of
        {{value, Pid}, Q2} ->
          Pool1 = Pool#pool{conn_pids = queue:in(Pid, Q2),
                            req_id = ((Pool#pool.req_id)+1)},
          Pools1 = [{PoolId, Pool1}|Others],
          {reply, {Pid, Pool}, State#state{pools=Pools1}};
        {empty, _} ->
          {reply, {undefined, Pool}, State}
      end
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
handle_info({'EXIT', _, normal}, State) ->
  {noreply, State};
handle_info({'EXIT', Pid, {emongo_conn, PoolId, Error}},
            #state{pools=Pools}=State) ->
  ?debugFmt("EXIT ~p, {emongo_conn, ~p, ~p} in ~p~n",
        [Pid, PoolId, Error, ?MODULE]),
  State1 =
    case get_pool(PoolId, Pools) of
      undefined ->
        State;
      {Pool, Others} ->
        Pids1 = queue:filter(fun(Item) -> Item =/= Pid end,
                             Pool#pool.conn_pids),
        Pool1 = Pool#pool{conn_pids = Pids1},
        case do_open_connections(Pool1) of
          {error, _Reason} ->
            Pools1 = Others;
          Pool2 ->
            Pools1 = [{PoolId, Pool2} | Others]
        end,
        State#state{pools=Pools1}
    end,
  {noreply, State1};

handle_info(Info, State) ->
  io:format("WARNING: unrecognized message in ~p: ~p~n", [?MODULE, Info]),
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
        Pool = #pool{
          id       = PoolId,
          size     = proplists:get_value(size, Props, 1),
          host     = proplists:get_value(host, Props, "localhost"),
          port     = proplists:get_value(port, Props, 27017),
          database = proplists:get_value(database, Props, "test")
        },
        {PoolId, do_open_connections(Pool)}
       end || {PoolId, Props} <- Pools]
  end.

do_open_connections(#pool{id        = PoolId,
                          host      = Host,
                          port      = Port,
                          size      = Size,
                          user      = User,
                          pass_hash = PassHash,
                          conn_pids = Pids} = Pool) ->
  case queue:len(Pids) < Size of
    true ->
      case emongo_conn:start_link(PoolId, Host, Port) of
        {error, Reason} ->
          throw({emongo_error, Reason});
        Pid ->
          do_auth(Pid, Pool, User, PassHash),
          do_open_connections(Pool#pool{conn_pids = queue:in(Pid, Pids)})
      end;
    false -> Pool
  end.

pass_hash(undefined, undefined) -> undefined;
pass_hash(User, Pass) ->
  emongo:dec2hex(erlang:md5(User ++ ":mongo:" ++ Pass)).

do_auth(_Pid, _Pool, undefined, undefined) -> ok;
do_auth(Pid, Pool, User, Hash) ->
  Nonce = case getnonce(Pid, Pool) of
    error -> throw(emongo_getnonce);
    N     -> N
  end,
  Digest = emongo:dec2hex(erlang:md5(binary_to_list(Nonce) ++ User ++ Hash)),
  Query = #emo_query{q=[{<<"authenticate">>, 1}, {<<"user">>, User},
                        {<<"nonce">>, Nonce}, {<<"key">>, Digest}], limit=1},
  Packet = emongo_packet:do_query(Pool#pool.database, "$cmd", Pool#pool.req_id,
                                  Query),
  Resp = send_recv_command(do_auth, undefined, undefined, undefined, Pid,
                           Pool#pool.req_id, Packet, ?TIMEOUT),
  [Res] = Resp#response.documents,
  case lists:keyfind(<<"ok">>, 1, Res) of
    {<<"ok">>, 1.0} -> {ok, authenticated};
    _ ->
      case lists:keyfind(<<"errmsg">>, 1, Res) of
        {<<"errmsg">>, Error} ->
          throw({emongo_authentication_failed, Error});
        _ ->
          throw({emongo_authentication_failed, <<"Unknown error">>})
      end
  end.

getnonce(Pid, Pool) ->
  Query1 = #emo_query{q=[{<<"getnonce">>, 1}], limit=1},
  Packet = emongo_packet:do_query(Pool#pool.database, "$cmd", Pool#pool.req_id,
                                  Query1),
  Resp1 = send_recv_command(getnonce, undefined, undefined, undefined, Pid,
                            Pool#pool.req_id, Packet, ?TIMEOUT),
  case lists:keyfind(<<"nonce">>, 1, lists:nth(1, Resp1#response.documents)) of
    false                -> error;
    {<<"nonce">>, Nonce} -> Nonce
  end.

get_pool(PoolId, Pools) ->
  get_pool(PoolId, Pools, []).

get_pool(_, [], _) ->
  undefined;

get_pool(PoolId, [{PoolId, Pool}|Tail], Others) ->
  {Pool, lists:append(Tail, Others)};

get_pool(PoolId, [Pool|Tail], Others) ->
  get_pool(PoolId, Tail, [Pool|Others]).

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

create_query(Options, SelectorIn) ->
  Selector = transform_selector(SelectorIn),
  EmoQuery = transform_options(Options, #emo_query{}),
  finalize_emo_query(Selector, EmoQuery).

finalize_emo_query(Selector, #emo_query{q = []} = EmoQuery) ->
  EmoQuery#emo_query{q = Selector};
finalize_emo_query([], EmoQuery) ->
  EmoQuery;
finalize_emo_query(Selector, #emo_query{q = Q} = EmoQuery) ->
  EmoQuery#emo_query{q = Q ++ [{<<"query">>, Selector}]}.

transform_options([], EmoQuery) -> EmoQuery;
transform_options([{limit, Limit} | Rest], EmoQuery) ->
  NewEmoQuery = EmoQuery#emo_query{limit=Limit},
  transform_options(Rest, NewEmoQuery);
transform_options([{offset, Offset} | Rest], EmoQuery) ->
  NewEmoQuery = EmoQuery#emo_query{offset=Offset},
  transform_options(Rest, NewEmoQuery);
transform_options([{orderby, OrderbyIn} | Rest],
                  #emo_query{q = Query} = EmoQuery) ->
  Orderby = {<<"orderby">>, [{Key, case Dir of desc -> -1; _ -> 1 end} ||
                             {Key, Dir} <- OrderbyIn]},
  NewEmoQuery = EmoQuery#emo_query{q = [Orderby | Query]},
  transform_options(Rest, NewEmoQuery);
transform_options([{fields, Fields} | Rest], EmoQuery) ->
  NewEmoQuery = EmoQuery#emo_query{field_selector=convert_fields(Fields)},
  transform_options(Rest, NewEmoQuery);
transform_options([Opt | Rest], #emo_query{opts = OptsIn} = EmoQuery)
    when is_integer(Opt) ->
  NewEmoQuery = EmoQuery#emo_query{opts = Opt bor OptsIn},
  transform_options(Rest, NewEmoQuery);
transform_options([{<<_/binary>>, _} = Option | Rest],
                  #emo_query{q = Query} = EmoQuery) ->
  NewEmoQuery = EmoQuery#emo_query{q = [Option | Query]},
  transform_options(Rest, NewEmoQuery);
transform_options([{Ignore, _} | Rest], EmoQuery) when Ignore == timeout ->
  transform_options(Rest, EmoQuery);
transform_options([Ignore | Rest], EmoQuery) when Ignore == check_match_found;
                                                  Ignore == response_options;
                                                  Ignore == ?USE_PRIMARY ->
  transform_options(Rest, EmoQuery);
transform_options([Invalid | _Rest], _EmoQuery) ->
  throw({emongo_invalid_option, Invalid}).

transform_selector(Selector) ->
  Res = lists:map(fun({Key, Value}) ->
    ConvKey = convert_key(Key),
    ForceDataType = force_data_type(ConvKey),
    ConvValue = convert_value(ForceDataType, Value),
    {ConvKey, ConvValue}
  end, Selector),
  %io:format("Res = ~p\n", [Res]),
  Res.

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
convert_key(elemMatch) -> <<"$elemMatch">>.

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

dec0($a) ->  10;
dec0($b) ->  11;
dec0($c) ->  12;
dec0($d) ->  13;
dec0($e) ->  14;
dec0($f) ->  15;
dec0(X) ->  X - $0.

hex0(10) -> $a;
hex0(11) -> $b;
hex0(12) -> $c;
hex0(13) -> $d;
hex0(14) -> $e;
hex0(15) -> $f;
hex0(I) ->  $0 + I.

sync_command(Command, Collection, Selector, Options, {Pid, Pool}, Packet1) ->
  Query1 = #emo_query{q=[{<<"getlasterror">>, 1}], limit=1},
  Packet2 = emongo_packet:do_query(Pool#pool.database, "$cmd", Pool#pool.req_id,
                                   Query1),
  Resp = try
    emongo_conn:send_sync(Pid, Pool#pool.req_id, Packet1, Packet2, ?TIMEOUT)
  catch _:{emongo_conn_error, Error} ->
    throw({emongo_conn_error, Error, Command, Collection, Selector, Options})
  end,
  case lists:member(response_options, Options) of
    true  -> Resp;
    false -> get_sync_result(Resp, lists:member(check_match_found, Options))
  end.

send_recv_command(Command, Collection, Selector, ExtraInfo, Pid, ReqID, Packet,
                  Timeout) ->
  try
    emongo_conn:send_recv(Pid, ReqID, Packet, Timeout)
  catch _:{emongo_conn_error, Error} ->
    throw({emongo_conn_error, Error, Command, Collection, Selector, ExtraInfo})
  end.

send_command(Command, Collection, Selector, ExtraInfo, Pid, ReqID, Packet) ->
  try
    emongo_conn:send(Pid, ReqID, Packet)
  catch _:{emongo_conn_error, Error} ->
    throw({emongo_conn_error, Error, Command, Collection, Selector, ExtraInfo})
  end.

get_sync_result(#response{documents = [Doc]}, CheckMatchFound) ->
  case lists:keyfind(<<"err">>, 1, Doc) of
    {_, undefined} -> check_match_found(Doc, CheckMatchFound);
    {_, Msg}       -> throw({emongo_error, Msg});
    _              -> throw({emongo_error, {invalid_error_message, Doc}})
  end;
get_sync_result(Resp, _CheckMatchFound) ->
  throw({emongo_error, {invalid_response, Resp}}).

check_match_found(_Doc, false) -> ok;
check_match_found(Doc, _) ->
  case lists:keyfind(<<"n">>, 1, Doc) of
    {_, 0} ->
      case lists:keyfind(<<"updatedExisting">>, 1, Doc) of
        {_, true} -> ok;
        _         ->  {emongo_no_match_found, Doc}
      end;
    {_, _} -> ok;
    false  -> throw({emongo_error, {invalid_response, Doc}})
  end.

to_binary(V) when is_binary(V) -> V;
to_binary(V) when is_list(V)   -> list_to_binary(V);
to_binary(V) when is_atom(V)   -> list_to_binary(atom_to_list(V)).

convert_fields(Fields) ->
  [{Field, 1} || Field <- Fields].

set_slave_ok(OptionsIn) ->
  case lists:member(?USE_PRIMARY, OptionsIn) of
    true -> OptionsIn;
    _    -> [?SLAVE_OK | OptionsIn]
  end.

% c("../../deps/emongo/src/emongo.erl", [{i, "../../deps/emongo/include"}]).
