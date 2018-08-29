-module(emongo_test).
-include("emongo.hrl").
-include("emongo_test.hrl").
-compile(export_all).

-define(NUM_PROCESSES,     100).
-define(NUM_TESTS_PER_PID, 500).
-define(POOL,              pool1).
-define(POOL_SIZE,         10).
-define(COLL,              <<"test">>).
-define(TIMEOUT,           60000).
-define(OUT(F, D),         ?debugFmt(F, D)).
-define(FUNCTION_NAME,     element(2, element(2, process_info(self(), current_function)))).
-define(STARTING,          ?OUT("~p", [?FUNCTION_NAME])).
-define(FIND_OPTIONS,      []).
-define(TEST_DATABASE,     <<"testdatabase">>).

run_test_() ->
  [{setup,
    fun ?MODULE:setup/0,
    fun ?MODULE:cleanup/1,
    [
      fun ?MODULE:test_upsert/0,
      fun ?MODULE:test_cursors/0,
      %fun ?MODULE:test_fetch_collections/0,
      fun ?MODULE:test_req_id_rollover/0,
      fun ?MODULE:test_timing/0,
      fun ?MODULE:test_drop_collection/0,
      fun ?MODULE:test_get_databases/0,
      fun ?MODULE:test_drop_database/0,
      fun ?MODULE:test_empty_sel_with_orderby/0,
      fun ?MODULE:test_count/0,
      fun ?MODULE:test_find_one/0,
      fun ?MODULE:test_read_preferences/0,
      fun ?MODULE:test_strip_selector/0,
      fun ?MODULE:test_duplicate_key_error/0,
      fun ?MODULE:test_bulk_insert/0,
      fun ?MODULE:test_update_sync/0,
      fun ?MODULE:test_distinct/0,
      fun ?MODULE:test_struct_syntax/0,
      fun ?MODULE:test_aggregate/0,
      fun ?MODULE:test_encoding_performance/0,
      {timeout, ?TIMEOUT div 1000, [fun ?MODULE:test_performance/0]}
    ]
  }].

setup() ->
  ensure_started(sasl),
  ensure_started(emongo),
  emongo:add_pool(?POOL, <<"localhost">>, 27017, ?TEST_DATABASE, ?POOL_SIZE),
  emongo:delete_sync(?POOL, ?COLL),
  ok.

cleanup(_) ->
  emongo:drop_database(?POOL),
  ok.

clear_coll() ->
  emongo:delete_sync(?POOL, ?COLL),
  ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

test_upsert() ->
  ?STARTING,
  Selector = [{<<"_id">>, <<"upsert_test">>}],
  UpsertRes1 = emongo:update_sync(?POOL, ?COLL, Selector,
                                  [{"$set", [{"data", 1}]}], true),
  ?assertEqual(1, UpsertRes1),
  Find1 = emongo:find_all(?POOL, ?COLL, Selector, ?FIND_OPTIONS),
  ?assertEqual([Selector ++ [{<<"data">>, 1}]], Find1),

  UpsertRes2 = emongo:update_sync(?POOL, ?COLL, Selector,
                                  [{"$set", [{"data", 2}]}], true),
  ?assertEqual(1, UpsertRes2),
  Find2 = emongo:find_all(?POOL, ?COLL, Selector, ?FIND_OPTIONS),
  ?assertEqual([Selector ++ [{<<"data">>, 2}]], Find2),
  clear_coll().

test_cursors() ->
  ?STARTING,
  emongo:insert_sync(?POOL, ?COLL, [
    [{<<"a">>, 9}],
    [{<<"a">>, 8}],
    [{<<"a">>, 7}],
    [{<<"a">>, 6}],
    [{<<"a">>, 5}],
    [{<<"a">>, 4}],
    [{<<"a">>, 3}],
    [{<<"a">>, 2}],
    [{<<"a">>, 1}],
    [{<<"a">>, 0}]
  ]),
  % Running emongo:find(...) ensures that the "limit" argument is working properly.
  ResSome = emongo:find(?POOL, ?COLL, [], [{fields, [{<<"_id">>, 0}]}, {limit, 5}, {orderby, [{<<"a">>, 1}]}]),
  ?assertEqual([
    [{<<"a">>, 0}],
    [{<<"a">>, 1}],
    [{<<"a">>, 2}],
    [{<<"a">>, 3}],
    [{<<"a">>, 4}]
  ], ResSome),
  % Running emongo:find_all(...) ensures that cursors are working because not all documents are returned in a single
  % call.
  ResAll = emongo:find_all(?POOL, ?COLL, [], [{fields, [{<<"_id">>, 0}]}, {limit, 5}, {orderby, [{<<"a">>, 1}]}]),
  ?assertEqual([
    [{<<"a">>, 0}],
    [{<<"a">>, 1}],
    [{<<"a">>, 2}],
    [{<<"a">>, 3}],
    [{<<"a">>, 4}],
    [{<<"a">>, 5}],
    [{<<"a">>, 6}],
    [{<<"a">>, 7}],
    [{<<"a">>, 8}],
    [{<<"a">>, 9}]
  ], ResAll),
  clear_coll().

% TODO: Why isn't this working?
%test_fetch_collections() ->
%  ?STARTING,
%  ok = emongo:insert_sync(?POOL, ?COLL, [{<<"a">>, 1}]),
%  Res = lists:sort(emongo:get_collections(?POOL, ?FIND_OPTIONS)),
%  ?OUT("Res = ~p", [Res]),
%  ?assertEqual([?COLL], Res),
%  clear_coll().

test_req_id_rollover() ->
  ?STARTING,
  {_, _} = gen_server:call(emongo, {conn, ?POOL, 2147483000}, infinity),
  {_, #pool{req_id = NextReq}} = gen_server:call(emongo, {conn, ?POOL, 1000000000}, infinity),
  ?assertEqual(1, NextReq).

test_timing() ->
  ?STARTING,
  emongo:clear_timing(),
  run_single_test(1, 1),
  ?OUT("DB Total Time: ~p usec",  [emongo:total_db_time_usec()]),
  ?OUT("DB Timing Breakdown: ~p", [emongo:db_timing()]),
  ?OUT("DB Queue Lengths: ~p",    [emongo:queue_lengths()]).

test_drop_collection() ->
  ?STARTING,
  ok = emongo:drop_collection(?POOL, ?COLL),
  ?assertEqual(false, lists:member(?COLL, emongo:get_collections(?POOL, ?FIND_OPTIONS))).

test_get_databases() ->
  ?STARTING,
  emongo:insert_sync(?POOL, ?COLL, [{<<"_id">>, <<"get_databases_test">>}]),
  Databases = lists:sort(emongo:get_databases(?POOL)),
  ?assert(lists:member(?TEST_DATABASE, Databases)),
  clear_coll().

test_drop_database() ->
  ?STARTING,
  ok = emongo:drop_database(?POOL).

test_empty_sel_with_orderby() ->
  ?STARTING,
  emongo:insert_sync(?POOL, ?COLL, [[{<<"a">>, 2}],
                                    [{<<"a">>, 1}]]),
  Res = emongo:find_all(?POOL, ?COLL, [], [{fields, [{<<"_id">>, 0}]}, {orderby, [{<<"a">>, 1}]} | ?FIND_OPTIONS]),
  ?assertEqual([[{<<"a">>, 1}],
                [{<<"a">>, 2}]], Res),
  clear_coll(),
  ?assertEqual([], emongo:find_all(?POOL, ?COLL, [], ?FIND_OPTIONS)).

test_count() ->
  ?STARTING,
  emongo:insert_sync(?POOL, ?COLL, [[{<<"a">>, 1}], [{<<"a">>, 2}], [{<<"a">>, 3}], [{<<"a">>, 4}], [{<<"a">>, 5}]]),
  %?OUT("Resp = ~p", [emongo:count(?POOL, ?COLL, [{<<"$or">>, [[{<<"a">>, [{lte, 2}]}], [{<<"a">>, [{gte, 4}]}]]}],
  %                                [response_options])]),
  ?assertEqual(5, emongo:count(?POOL, ?COLL, [], ?FIND_OPTIONS)),
  ?assertEqual(3, emongo:count(?POOL, ?COLL, [{<<"a">>, [{lte, 3}]}], ?FIND_OPTIONS)),
  ?assertEqual(2, emongo:count(?POOL, ?COLL, [{<<"a">>, [{gt,  3}]}], ?FIND_OPTIONS)),
  ?assertEqual(4, emongo:count(?POOL, ?COLL, [{<<"$or">>,  [[{<<"a">>, [{lte, 2}]}], [{<<"a">>, [{gte, 4}]}]]}],
                               ?FIND_OPTIONS)),
  ?assertEqual(3, emongo:count(?POOL, ?COLL, [{<<"$and">>, [[{<<"a">>, [{gte, 2}]}], [{<<"a">>, [{lte, 4}]}]]}],
                               ?FIND_OPTIONS)),
  clear_coll().

test_find_one() ->
  ?STARTING,
  emongo:insert_sync(?POOL, ?COLL, [[{<<"a">>, 1}], [{<<"a">>, 2}], [{<<"a">>, 2}], [{<<"a">>, 3}], [{<<"a">>, 3}]]),
  ?assertEqual(1, length(emongo:find_one(?POOL, ?COLL, [{<<"a">>, 2}], ?FIND_OPTIONS))),
  clear_coll().

test_read_preferences() ->
  ?STARTING,
  ok = emongo:insert_sync(?POOL, ?COLL, [{<<"a">>, 1}]),
  ?assertEqual([[{<<"a">>, 1}]], emongo:find_all(?POOL, ?COLL, [], [{fields, [{<<"_id">>, 0}]}])),
  ?assertEqual([[{<<"a">>, 1}]], emongo:find_all(?POOL, ?COLL, [], [{fields, [{<<"_id">>, 0}]}, ?SLAVE_OK])),
  ?assertEqual([[{<<"a">>, 1}]], emongo:find_all(?POOL, ?COLL, [], [{fields, [{<<"_id">>, 0}]}, ?USE_PRIMARY])),
  ?assertEqual([[{<<"a">>, 1}]], emongo:find_all(?POOL, ?COLL, [], [{fields, [{<<"_id">>, 0}]}, ?USE_PRIM_PREF])),
  ?assertEqual([[{<<"a">>, 1}]], emongo:find_all(?POOL, ?COLL, [], [{fields, [{<<"_id">>, 0}]}, ?USE_SECONDARY])),
  ?assertEqual([[{<<"a">>, 1}]], emongo:find_all(?POOL, ?COLL, [], [{fields, [{<<"_id">>, 0}]}, ?USE_SECD_PREF])),
  ?assertEqual([[{<<"a">>, 1}]], emongo:find_all(?POOL, ?COLL, [], [{fields, [{<<"_id">>, 0}]}, ?USE_NEAREST])),
  clear_coll().

test_strip_selector() ->
  ?STARTING,
  ?assertEqual([{<<"a">>, undefined}],
               emongo:strip_selector([{<<"a">>, 1}])),
  ?assertEqual([{<<"a">>, undefined}, {<<"b">>, undefined}],
               emongo:strip_selector([{<<"a">>, 1}, {<<"b">>, 1}])),
  ?assertEqual([{<<"$or">>, {array, [[{<<"a">>, undefined}], [{<<"a">>, undefined}]]}}],
               emongo:strip_selector([{<<"$or">>, {array, [[{<<"a">>, 1}], [{<<"a">>, 2}]]}}])),
  ?assertEqual([{<<"markets">>, undefined}],
               emongo:strip_selector([{<<"markets">>, [{<<"$in">>, {array, [<<"some thing">>, <<"ALL-MARKETS">>]}}]}])),
  ?assertEqual([{<<"markets">>, undefined}],
               emongo:strip_selector([{<<"markets">>, [{<<"$in">>, {array, []}}]}])),
  ?assertEqual([{<<"a">>, undefined}],
               emongo:strip_selector([{<<"a">>, [{<<"$in">>, [1,2,3]}]}])),
  ?assertEqual([{<<"a">>, [{<<"$elemMatch">>, [{<<"b">>, undefined}]}]}],
               emongo:strip_selector([{<<"a">>, [{<<"$elemMatch">>, [{<<"b">>, 1}]}]}])),
  ?assertEqual([{<<"a">>, undefined}],
               emongo:strip_selector([{<<"a">>, [{<<"$exists">>, true}]}])),
  % This is a ridiculous selector we have in our system that I'm including just for fun:
  Stripped1 = emongo:strip_selector([
    {<<"a">>, 1},
    {<<"$and">>, {array, [
      [{<<"$or">>, {array, [
        [{<<"status">>, undefined}],
        [{<<"status">>, <<"active">>}]
      ]}}],
      [{<<"$or">>, {array, [
        [{<<"start_date_time">>, undefined}],
        [{<<"start_date_time">>, [{<<"$lte">>, 123}]}]
      ]}}],
      [{<<"$or">>, {array, [
        [{<<"end_date_time">>, undefined}],
        [{<<"end_date_time">>, [{<<"$gte">>, 123}]}]
      ]}}]
    ]}}
  ]),
  ExpectedRes1 = [
    {<<"a">>, undefined},
    {<<"$and">>, {array, [
      [{<<"$or">>, {array, [
        [{<<"status">>, undefined}],
        [{<<"status">>, undefined}]
      ]}}],
      [{<<"$or">>, {array, [
        [{<<"start_date_time">>, undefined}],
        [{<<"start_date_time">>, undefined}]
      ]}}],
      [{<<"$or">>, {array, [
        [{<<"end_date_time">>, undefined}],
        [{<<"end_date_time">>, undefined}]
      ]}}]
    ]}}],
  ?assertEqual(ExpectedRes1, Stripped1),
  % This is another ridiculous selector we have in our system that I'm including just for fun:
  Stripped2 = emongo:strip_selector([
    {<<"r">>, <<"Denver">>},
    {<<"h">>, 12345},
    {<<"s">>, 1},
    {<<"status">>, <<"online">>},
    {<<"avail">>, [{<<"$gte">>, 12345}]},
    {<<"mp">>, [{<<"$elemMatch">>, [
      {<<"avail">>, true},
      {<<"time">>,  [{"$lte", 12345}]}
    ]}]}
  ]),
  ExpectedRes2 = [
    {<<"r">>, undefined},
    {<<"h">>, undefined},
    {<<"s">>, undefined},
    {<<"status">>, undefined},
    {<<"avail">>, undefined},
    {<<"mp">>, [{<<"$elemMatch">>, [
      {<<"avail">>, undefined},
      {<<"time">>,  undefined}
    ]}]}
  ],
  ?assertEqual(ExpectedRes2, Stripped2).

test_duplicate_key_error() ->
  ?STARTING,
  emongo:create_index(?POOL, ?COLL, [{<<"a">>, 1}], [{<<"unique">>, true}]),
  emongo:insert_sync(?POOL, ?COLL, [[{<<"a">>, 1}], [{<<"a">>, 2}]]),
  ?assertThrow({emongo_error, duplicate_key, _},
    emongo:find_and_modify(?POOL, ?COLL, [{<<"a">>, 1}], [{<<"$set">>, [{<<"a">>, 2}]}], [{new, true}])),
  ?assertThrow({emongo_error, duplicate_key, _},
    emongo:update_sync(?POOL, ?COLL, [{<<"a">>, 1}], [{<<"$set">>, [{<<"a">>, 2}]}])),
  clear_coll(),
  emongo:drop_index(?POOL, ?COLL, <<"a_1">>).

test_bulk_insert() ->
  ?STARTING,
  Count = 5000,

  % Create an index used for creating writeErrors
  emongo:create_index(?POOL, ?COLL, [{<<"index">>, 1}], [{<<"unique">>, true}]),

  % Insert 5000 documents - emongo will break down into 5 calls
  Docs = lists:map(fun(N) ->
           ?SMALL_DOCUMENT(N, <<"bulk_insert">>)
         end, lists:seq(1, Count)),

  IRes = emongo:insert_sync(?POOL, ?COLL, Docs, [response_options]),
  #response{documents=[IResDoc]} = IRes,

  % Check that ok and n values are aggregated
  ?assertEqual(1.0, proplists:get_value(<<"ok">>, IResDoc)),
  ?assertEqual(Count, proplists:get_value(<<"n">>, IResDoc)),

  % Check that writeErrors are aggregated
  IErrorRes = emongo:insert_sync(?POOL, ?COLL, Docs, [response_options, {ordered, false}]),
  #response{documents=[IErrorResDoc]} = IErrorRes,
  {array, WriteErrors} = proplists:get_value(<<"writeErrors">>, IErrorResDoc),
  ?assertEqual(Count, length(WriteErrors)),

  emongo:drop_index(?POOL, ?COLL, <<"index_1">>),
  clear_coll().

test_update_sync() ->
  ?STARTING,
  ?assertEqual([], emongo:find_all(?POOL, ?COLL, [{<<"a">>, 1}])),
  ?assertEqual(0, emongo:update_sync(?POOL, ?COLL, [{<<"a">>, 1}], [{<<"$set">>, [{<<"a">>, 1}]}])),
  ok = emongo:insert_sync(?POOL, ?COLL, [{<<"a">>, 1}]),
  ?assertEqual(1, emongo:update_sync(?POOL, ?COLL, [{<<"a">>, 1}], [{<<"$set">>, [{<<"a">>, 1}]}])),
  ok = clear_coll().

test_distinct() ->
  ?STARTING,
  lists:foreach(fun(X) ->
    emongo:insert_sync(?POOL, ?COLL, [{<<"a">>, [{<<"b">>, X rem 5}]}, {<<"c">>, X rem 50}])
  end, lists:seq(1, 100)),
  ?assertEqual([0, 1, 2, 3, 4], lists:sort(emongo:distinct(?POOL, ?COLL, <<"a.b">>))),
  ?assertEqual([0],             lists:sort(emongo:distinct(?POOL, ?COLL, <<"a.b">>, [{<<"c">>, 0}], []))),
  ?assertEqual([0],             lists:sort(emongo:distinct(?POOL, ?COLL, <<"a.b">>, [{<<"c">>, 0}], [?USE_SECD_PREF]))),
  clear_coll().

test_struct_syntax() ->
  ?STARTING,
  Selector = {struct, [{<<"_id">>, {struct, [{<<"a">>, <<"struct_syntax_test">>}]}}]},
  ?assertEqual(true, ?IS_DOCUMENT(Selector)),
  ?assertEqual(true, ?IS_LIST_OF_DOCUMENTS([Selector])),
  emongo:update_sync(?POOL, ?COLL, Selector, [{"$set", [{"data", 1}]}], true),
  Find = emongo:find_all(?POOL, ?COLL, Selector, ?FIND_OPTIONS),
  ?assertEqual([[{<<"_id">>, [{<<"a">>, <<"struct_syntax_test">>}]}, {<<"data">>, 1}]], Find),
  clear_coll().

test_aggregate() ->
  ?STARTING,
  emongo:insert_sync(?POOL, ?COLL, [
    [{<<"a">>, 1}, {<<"b">>, 1}],
    [{<<"a">>, 1}, {<<"b">>, 2}],
    [{<<"a">>, 1}, {<<"b">>, 3}],
    [{<<"a">>, 2}, {<<"b">>, 4}],
    [{<<"a">>, 2}, {<<"b">>, 5}]
  ]),
  Res = emongo:aggregate(?POOL, ?COLL, [
    [{<<"$match">>, [{<<"b">>, [{<<"$gte">>, 2}]}]}],
    [{<<"$group">>, [
      {<<"_id">>, <<"$a">>},
      {<<"sum_b">>, [{<<"$sum">>, <<"$b">>}]}
    ]}]
  ], [{batch_size, 1}]),
  ?assertEqual([
    [{<<"_id">>, 1}, {<<"sum_b">>, 5}],
    [{<<"_id">>, 2}, {<<"sum_b">>, 9}]
  ], lists:sort(Res)),
  clear_coll().

test_encoding_performance() ->
  ?STARTING,
  {EncodeTime, ok} = timer:tc(fun() ->
    lists:foreach(fun(_) ->
      <<_/binary>> = emongo_bson:encode(?REALLY_BIG_DOCUMENT)
    end, lists:seq(1, 1000))
  end),
  ?OUT("Encoding a really big document 1000 times took ~p microseconds", [EncodeTime]),
  {WriteTime, ok} = timer:tc(fun() ->
    lists:foreach(fun(_) ->
      emongo:insert_sync(?POOL, ?COLL, ?REALLY_BIG_DOCUMENT)
    end, lists:seq(1, 1000))
  end),
  ?OUT("Encoding and writing a really big document to DB 1000 times took ~p microseconds", [WriteTime]),
  clear_coll().

test_performance() ->
  ?STARTING,
  Ref = make_ref(),
  start_processes(Ref),
  block_until_done(Ref),
  clear_coll().

start_processes(Ref) ->
  Pid = self(),
  lists:foreach(fun(X) ->
    proc_lib:spawn(fun() ->
      lists:map(fun(Y) ->
        run_single_test(X, Y)
      end, lists:seq(1, ?NUM_TESTS_PER_PID)),
      Pid ! {Ref, done}
    end)
  end, lists:seq(1, ?NUM_PROCESSES)).

run_single_test(X, Y) ->
  Num = (X bsl 16) bor Y, % Make up a unique number for this run
  Selector = [{<<"_id">>, Num}],
  try
    IRes = emongo:insert_sync(?POOL, ?COLL, Selector, [response_options]),
    ok = check_result(insert_sync, IRes, 0),

    [FMRes] = emongo:find_and_modify(?POOL, ?COLL, Selector,
      [{<<"$set">>, [{<<"fm">>, Num}]}], [{new, true}]),
    FMVal = proplists:get_value(<<"value">>, FMRes),
    ?assertEqual(Selector ++ [{<<"fm">>, Num}], FMVal),

    URes = emongo:update_sync(?POOL, ?COLL, Selector,
      [{<<"$set">>, [{<<"us">>, Num}]}], false, [response_options]),
    ok = check_result(update_sync, URes, 1),

    FARes = emongo:find_all(?POOL, ?COLL, Selector, ?FIND_OPTIONS),
    ?assertEqual([Selector ++ [{<<"fm">>, Num}, {<<"us">>, Num}]], FARes),

    DRes = emongo:delete_sync(?POOL, ?COLL, Selector, [response_options]),
    ok = check_result(delete_sync, DRes, 1)
  catch _:E ->
    ?OUT("Exception occurred for test ~.16b: ~p\n~p\n",
              [Num, E, erlang:get_stacktrace()]),
    throw(test_failed)
  end.

check_result(Desc,
             {response, _,_,_,_,_, [List]},
             ExpectedN) when is_list(List) ->
  Ok  = round(proplists:get_value(<<"ok">>, List)),
  Err = proplists:get_value(<<"err">>, List),
  N   = round(proplists:get_value(<<"n">>, List)),
  if Err == undefined, N == ExpectedN -> ok;
     Ok == 1, Err == undefined, Desc == insert_sync, N == 1 -> ok;
     Ok == 1, Err == undefined, N == ExpectedN -> ok;
     true ->
       ?OUT("Unexpected result for ~p: Ok = ~p; Err = ~p; N = ~p", [Desc, Ok, Err, N]),
       throw({error, invalid_db_response})
  end.

block_until_done(Ref) ->
  block_until_done(Ref, 0).

block_until_done(_, ?NUM_PROCESSES) -> ok;
block_until_done(Ref, NumDone) ->
  ToAdd =
    receive {Ref, done} -> 1
    after 1000 ->
      ?OUT("DB Queue Lengths: ~p", [emongo:queue_lengths()]),
      0
    end,
  block_until_done(Ref, NumDone + ToAdd).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

ensure_started(App) ->
  case application:start(App) of
    ok -> ok;
    {error, {already_started, App}} -> ok
  end.

cur_time_ms() ->
  erlang:system_time(seconds).
