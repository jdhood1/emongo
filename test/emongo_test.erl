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
-define(TEST_OUT(F, D),    ?debugFmt(F, D)).
-define(FUNCTION_NAME,     element(2, element(2, process_info(self(), current_function)))).
-define(STARTING,          OutputStartTimeMs = cur_time_ms(), ?TEST_OUT("~p", [?FUNCTION_NAME])).
-define(ENDING,            clear_coll(), ?TEST_OUT("Test completed in ~p ms.", [cur_time_ms() - OutputStartTimeMs])).
-define(TEST_DATABASE,     <<"testdatabase">>).

run_test_() ->
  [{setup,
    fun ?MODULE:setup/0,
    fun ?MODULE:cleanup/1,
    [
      fun ?MODULE:test_find/0,
      fun ?MODULE:test_update_sync/0,
      fun ?MODULE:test_upsert/0,
      %fun ?MODULE:test_fetch_collections/0,
      fun ?MODULE:test_req_id_rollover/0,
      fun ?MODULE:test_timing/0,
      fun ?MODULE:test_drop_collection/0,
      fun ?MODULE:test_get_databases/0,
      fun ?MODULE:test_drop_database/0,
      fun ?MODULE:test_empty_sel_with_orderby/0,
      fun ?MODULE:test_count/0,
      fun ?MODULE:test_read_preferences/0,
      fun ?MODULE:test_strip_selector/0,
      fun ?MODULE:test_duplicate_key_error/0,
      fun ?MODULE:test_bulk_insert/0,
      fun ?MODULE:test_distinct/0,
      fun ?MODULE:test_struct_syntax/0,
      fun ?MODULE:test_aggregate/0,
      fun ?MODULE:test_encoding_performance/0,
      fun ?MODULE:test_config_performance/0,
      fun ?MODULE:test_lots_of_documents/0,
      fun ?MODULE:test_large_data/0,
      {timeout, ?TIMEOUT div 1000, [fun ?MODULE:test_performance/0]}
    ]
  }].

setup() ->
  ensure_started(sasl),
  ensure_started(emongo),
  Options = [
    {max_pipeline_depth,    0},
    {socket_options,        [{nodelay, false}]},
    {write_concern,         1},
    {write_concern_timeout, 4000},
    {disconnect_timeouts,   10},
    {default_read_pref,     <<"secondaryPreferred">>},
    {max_batch_size,        0}
  ],
  emongo:add_pool(?POOL, <<"localhost">>, 27017, ?TEST_DATABASE, ?POOL_SIZE, Options),
  emongo:delete_sync(?POOL, ?COLL),
  ok.

cleanup(_) ->
  emongo:drop_database(?POOL),
  ok.

clear_coll() ->
  emongo:delete_sync(?POOL, ?COLL),
  ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

test_find() ->
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
  Res1 = emongo:find(?POOL, ?COLL, [{<<"a">>, [{<<"$lte">>, 4}]}],
                     [{limit, 2}, {orderby, [{<<"a">>, 1}]}, {fields, [{<<"_id">>, 0}]}]),
  ?assertEqual([
    [{<<"a">>, 0}],
    [{<<"a">>, 1}]
  ], Res1),
  Res2 = emongo:find(?POOL, ?COLL, [{<<"a">>, [{<<"$lte">>, 4}]}],
                     [{orderby, [{<<"a">>, asc}]}, {fields, [{<<"_id">>, 0}]}]),
  ?assertEqual([
    [{<<"a">>, 0}],
    [{<<"a">>, 1}],
    [{<<"a">>, 2}],
    [{<<"a">>, 3}],
    [{<<"a">>, 4}]
  ], Res2),
  PrevMaxBatchSize = emongo:get_config(?POOL, max_batch_size),
  emongo:update_pool_options(?POOL, [{max_batch_size, 2}]),
  Res3 = emongo:find(?POOL, ?COLL, [], [{orderby, [{<<"a">>, asc}]}, {fields, [{<<"_id">>, 0}]}]),
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
  ], Res3),
  emongo:update_pool_options(?POOL, [{max_batch_size, PrevMaxBatchSize}]),
  Res4 = emongo:find(?POOL, ?COLL, [], [{limit, 2}, {orderby, [{<<"a">>, desc}]}, {fields, [{<<"_id">>, 0}]}]),
  ?assertEqual([
    [{<<"a">>, 9}],
    [{<<"a">>, 8}]
  ], Res4),
  Res5 = emongo:find_one(?POOL, ?COLL, [], [{limit, 2}, {orderby, [{<<"a">>, -1}]}, {fields, [{<<"_id">>, 0}]}]),
  ?assertEqual([
    [{<<"a">>, 9}]
  ], Res5),
  Res6 = emongo:find_all(?POOL, ?COLL, [], [{limit, 2}, {orderby, [{<<"a">>, desc}]}, {fields, [{<<"_id">>, 0}]}]),
  ?assertEqual([
    [{<<"a">>, 9}],
    [{<<"a">>, 8}]
  ], Res6),
  ?ENDING.

test_update_sync() ->
  ?STARTING,
  ?assertEqual([], emongo:find_all(?POOL, ?COLL, [{<<"a">>, 1}])),
  ?assertEqual(0, emongo:update_sync(?POOL, ?COLL, [{<<"a">>, 1}], [{<<"$set">>, [{<<"a">>, 1}]}])),
  ok = emongo:insert_sync(?POOL, ?COLL, [{<<"a">>, 1}]),
  ?assertEqual(1, emongo:update_sync(?POOL, ?COLL, [{<<"a">>, 1}], [{<<"$set">>, [{<<"a">>, 1}]}])),
  ?ENDING.

test_upsert() ->
  ?STARTING,
  Selector = [{<<"_id">>, <<"upsert_test">>}],
  UpsertRes1 = emongo:update_sync(?POOL, ?COLL, Selector,
                                  [{"$set", [{"data", 1}]}], true),
  ?assertEqual(1, UpsertRes1),
  Find1 = emongo:find(?POOL, ?COLL, Selector),
  ?assertEqual([Selector ++ [{<<"data">>, 1}]], Find1),

  UpsertRes2 = emongo:update_sync(?POOL, ?COLL, Selector,
                                  [{"$set", [{"data", 2}]}], true),
  ?assertEqual(1, UpsertRes2),
  Find2 = emongo:find(?POOL, ?COLL, Selector),
  ?assertEqual([Selector ++ [{<<"data">>, 2}]], Find2),
  ?ENDING.

% TODO: Why isn't this working?
%test_fetch_collections() ->
%  ?STARTING,
%  ok = emongo:insert_sync(?POOL, ?COLL, [{<<"a">>, 1}]),
%  Res = lists:sort(emongo:get_collections(?POOL)),
%  ?TEST_OUT("Res = ~p", [Res]),
%  ?assertEqual([?COLL], Res),
%  ?ENDING.

test_req_id_rollover() ->
  ?STARTING,
  {_, _} = gen_server:call(emongo, {conn, ?POOL, 2147483000}, infinity),
  {_, #pool{req_id = NextReq}} = gen_server:call(emongo, {conn, ?POOL, 1000000000}, infinity),
  ?assertEqual(1, NextReq),
  ?ENDING.

test_timing() ->
  ?STARTING,
  emongo:clear_timing(),
  run_single_test(1, 1),
  ?TEST_OUT("DB Total Time: ~p usec",  [emongo:total_db_time_usec()]),
  ?TEST_OUT("DB Timing Breakdown: ~p", [emongo:db_timing()]),
  ?TEST_OUT("DB Queue Lengths: ~p",    [emongo:queue_lengths()]),
  ?ENDING.

test_drop_collection() ->
  ?STARTING,
  ok = emongo:drop_collection(?POOL, ?COLL),
  ?assertEqual(false, lists:member(?COLL, emongo:get_collections(?POOL))),
  ?ENDING.

test_get_databases() ->
  ?STARTING,
  emongo:insert_sync(?POOL, ?COLL, [{<<"_id">>, <<"get_databases_test">>}]),
  Databases = lists:sort(emongo:get_databases(?POOL)),
  ?assert(lists:member(?TEST_DATABASE, Databases)),
  ?ENDING.

test_drop_database() ->
  ?STARTING,
  ok = emongo:drop_database(?POOL),
  ?ENDING.

test_empty_sel_with_orderby() ->
  ?STARTING,
  emongo:insert_sync(?POOL, ?COLL, [[{<<"a">>, 2}],
                                    [{<<"a">>, 1}]]),
  Res = emongo:find_all(?POOL, ?COLL, [], [{fields, [{<<"_id">>, 0}]}, {orderby, [{<<"a">>, 1}]}]),
  ?assertEqual([[{<<"a">>, 1}],
                [{<<"a">>, 2}]], Res),
  clear_coll(),
  ?assertEqual([], emongo:find_all(?POOL, ?COLL, [])),
  ?ENDING.

test_count() ->
  ?STARTING,
  emongo:insert_sync(?POOL, ?COLL, [[{<<"a">>, 1}], [{<<"a">>, 2}], [{<<"a">>, 3}], [{<<"a">>, 4}], [{<<"a">>, 5}]]),
  %?TEST_OUT("Resp = ~p", [emongo:count(?POOL, ?COLL, [{<<"$or">>, [[{<<"a">>, [{lte, 2}]}], [{<<"a">>, [{gte, 4}]}]]}],
  %                                     [response_options])]),
  ?assertEqual(5, emongo:count(?POOL, ?COLL, [])),
  ?assertEqual(3, emongo:count(?POOL, ?COLL, [{<<"a">>, [{lte, 3}]}])),
  ?assertEqual(2, emongo:count(?POOL, ?COLL, [{<<"a">>, [{gt,  3}]}])),
  ?assertEqual(4, emongo:count(?POOL, ?COLL, [{<<"$or">>,  [[{<<"a">>, [{lte, 2}]}], [{<<"a">>, [{gte, 4}]}]]}])),
  ?assertEqual(3, emongo:count(?POOL, ?COLL, [{<<"$and">>, [[{<<"a">>, [{gte, 2}]}], [{<<"a">>, [{lte, 4}]}]]}])),
  ?ENDING.

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
  ?ENDING.

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
  ?assertEqual(ExpectedRes2, Stripped2),
  ?ENDING.

test_duplicate_key_error() ->
  ?STARTING,
  emongo:create_index(?POOL, ?COLL, [{<<"a">>, 1}], [{<<"unique">>, true}]),
  emongo:insert_sync(?POOL, ?COLL, [[{<<"a">>, 1}], [{<<"a">>, 2}]]),
  ?assertThrow({emongo_error, duplicate_key, _},
    emongo:find_and_modify(?POOL, ?COLL, [{<<"a">>, 1}], [{<<"$set">>, [{<<"a">>, 2}]}], [{new, true}])),
  ?assertThrow({emongo_error, duplicate_key, _},
    emongo:update_sync(?POOL, ?COLL, [{<<"a">>, 1}], [{<<"$set">>, [{<<"a">>, 2}]}])),
  emongo:drop_index(?POOL, ?COLL, <<"a_1">>),
  ?ENDING.

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
  ?ENDING.

test_distinct() ->
  ?STARTING,
  lists:foreach(fun(X) ->
    emongo:insert_sync(?POOL, ?COLL, [{<<"a">>, [{<<"b">>, X rem 5}]}, {<<"c">>, X rem 50}])
  end, lists:seq(1, 100)),
  ?assertEqual([0, 1, 2, 3, 4], lists:sort(emongo:distinct(?POOL, ?COLL, <<"a.b">>))),
  ?assertEqual([0],             lists:sort(emongo:distinct(?POOL, ?COLL, <<"a.b">>, [{<<"c">>, 0}], []))),
  ?assertEqual([0],             lists:sort(emongo:distinct(?POOL, ?COLL, <<"a.b">>, [{<<"c">>, 0}], [?USE_SECD_PREF]))),
  ?ENDING.

test_struct_syntax() ->
  ?STARTING,
  Selector = {struct, [{<<"_id">>, {struct, [{<<"a">>, <<"struct_syntax_test">>}]}}]},
  ?assertEqual(true, ?IS_DOCUMENT(Selector)),
  ?assertEqual(true, ?IS_LIST_OF_DOCUMENTS([Selector])),
  emongo:update_sync(?POOL, ?COLL, Selector, [{"$set", [{"data", 1}]}], true),
  Find = emongo:find_all(?POOL, ?COLL, Selector),
  ?assertEqual([[{<<"_id">>, [{<<"a">>, <<"struct_syntax_test">>}]}, {<<"data">>, 1}]], Find),
  ?ENDING.

test_aggregate() ->
  ?STARTING,
  emongo:insert_sync(?POOL, ?COLL, [
    [{<<"a">>, 1}, {<<"b">>, 1}],
    [{<<"a">>, 1}, {<<"b">>, 2}],
    [{<<"a">>, 1}, {<<"b">>, 3}],
    [{<<"a">>, 2}, {<<"b">>, 4}],
    [{<<"a">>, 2}, {<<"b">>, 5}]
  ]),
  Aggregate = [
    [{<<"$match">>, [{<<"b">>, [{<<"$gte">>, 2}]}]}],
    [{<<"$group">>, [
      {<<"_id">>, <<"$a">>},
      {<<"sum_b">>, [{<<"$sum">>, <<"$b">>}]}
    ]}],
    [{<<"$sort">>, [{<<"sum_b">>, -1}]}]
  ],
  Res1 = (catch emongo:aggregate(?POOL, ?COLL, Aggregate)),
  ?assertEqual([
    [{<<"_id">>, 2}, {<<"sum_b">>, 9}],
    [{<<"_id">>, 1}, {<<"sum_b">>, 5}]
  ], Res1),
  Res2 = (catch emongo:aggregate(?POOL, ?COLL, Aggregate, [{limit, 1}])),
  %?TEST_OUT("Res2 = ~p", [Res2]),
  ?assertEqual([
    [{<<"_id">>, 2}, {<<"sum_b">>, 9}]
  ], Res2),
  ?ENDING.

test_encoding_performance() ->
  ?STARTING,
  {EncodeTime, ok} = timer:tc(fun() ->
    lists:foreach(fun(_) ->
      <<_/binary>> = emongo_bson:encode(?REALLY_BIG_DOCUMENT)
    end, lists:seq(1, 1000))
  end),
  ?TEST_OUT("Encoding a really big document 1000 times took ~p microseconds", [EncodeTime]),
  {WriteTime, ok} = timer:tc(fun() ->
    lists:foreach(fun(_) ->
      emongo:insert_sync(?POOL, ?COLL, ?REALLY_BIG_DOCUMENT)
    end, lists:seq(1, 1000))
  end),
  ?TEST_OUT("Encoding and writing a really big document to DB 1000 times took ~p microseconds", [WriteTime]),
  ?ENDING.

test_config_performance() ->
  ?STARTING,
  NumTests = 1000000,
  StartTimeMs = cur_time_ms(),
  lists:foreach(fun(_) ->
    ok
  end, lists:seq(1, NumTests)),
  MidTimeMs = cur_time_ms(),
  lists:foreach(fun(_) ->
    emongo:get_config(?POOL, max_batch_size)
  end, lists:seq(1, NumTests)),
  EndTimeMs = cur_time_ms(),
  OverheadTime = MidTimeMs - StartTimeMs,
  TestTime     = EndTimeMs - MidTimeMs,
  ConfigTime   = TestTime - OverheadTime,
  ?TEST_OUT("~p config calls were made in ~p ms.", [NumTests, ConfigTime]),
  ?ENDING.

test_lots_of_documents() ->
  ?STARTING,
  Data = <<0:(8*2000)>>,
  Docs = [[{<<"a">>, X}, {<<"data">>, Data}] || X <- lists:seq(1, 5000)],
  ok   = emongo:insert_sync(?POOL, ?COLL, Docs),
  Res  = emongo:find(?POOL, ?COLL, [], [{fields, [{<<"_id">>, 0}]}, {orderby, [{<<"a">>, 1}]}]),
  ?assertEqual(Docs, Res),
  ?ENDING.

test_large_data() ->
  ?STARTING,
  Data = <<0:(8*2*1048576)>>, % 2 MB
  Docs = [[{<<"a">>, X}, {<<"data">>, Data}] || X <- lists:seq(1, 50)],
  % TODO: Once bulk insert limits messages to MongoDB to 16 MB, go back to doing a bulk insert here:
  %ok   = emongo:insert_sync(?POOL, ?COLL, Docs),
  lists:foreach(fun(Doc) ->
    emongo:insert_sync(?POOL, ?COLL, Doc)
  end, Docs),
  Res  = emongo:find(?POOL, ?COLL, [], [{fields, [{<<"_id">>, 0}]}]),
  %?TEST_OUT("Res = ~p", [Res]),
  ?assertEqual(Docs, Res),
  ?ENDING.

test_performance() ->
  ?STARTING,
  PrevMaxBatchSize = emongo:get_config(?POOL, max_batch_size),
  emongo:update_pool_options(?POOL, [{max_batch_size, 101}]),
  Ref = make_ref(),
  start_processes(Ref),
  block_until_done(Ref),
  emongo:update_pool_options(?POOL, [{max_batch_size, PrevMaxBatchSize}]),
  ?ENDING.

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

    FARes = emongo:find_all(?POOL, ?COLL, Selector),
    ?assertEqual([Selector ++ [{<<"fm">>, Num}, {<<"us">>, Num}]], FARes),

    DRes = emongo:delete_sync(?POOL, ?COLL, Selector, [response_options]),
    ok = check_result(delete_sync, DRes, 1)
  catch _:E ->
    ?TEST_OUT("Exception occurred for test ~.16b: ~p\n~p\n", [Num, E, erlang:get_stacktrace()]),
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
       ?TEST_OUT("Unexpected result for ~p: Ok = ~p; Err = ~p; N = ~p", [Desc, Ok, Err, N]),
       throw({error, invalid_db_response})
  end.

block_until_done(Ref) ->
  block_until_done(Ref, 0).

block_until_done(_, ?NUM_PROCESSES) -> ok;
block_until_done(Ref, NumDone) ->
  ToAdd =
    receive {Ref, done} -> 1
    after 1000 ->
      ?TEST_OUT("DB Queue Lengths: ~p", [emongo:queue_lengths()]),
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
  os:system_time(milli_seconds).
