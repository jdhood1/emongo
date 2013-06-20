-module(emongo_test).
-include_lib("eunit/include/eunit.hrl").
-compile(export_all).

-define(NUM_PROCESSES,     100).
-define(NUM_TESTS_PER_PID, 500).
-define(POOL,              pool1).
-define(COLL,              <<"test">>).
-define(TIMEOUT,           60000).
-define(OUT(F, D),         ?debugFmt(F, D)).

setup() ->
  ensure_started(sasl),
  ensure_started(emongo),
  emongo:add_pool(?POOL, "localhost", 27017, "testdatabase", 10),
                  %"test_username", "test_password"),
  emongo:delete_sync(?POOL, ?COLL),
  ok.

cleanup(_) ->
  emongo:drop_database(?POOL),
  ok.

run_test_() ->
  [{setup,
    fun setup/0,
    fun cleanup/1,
    [
      fun test_upsert/0,
      fun test_fetch_collections/0,
      {timeout, ?TIMEOUT div 1000, [fun test_performance/0]},
      fun test_drop_collection/0,
      fun test_drop_database/0,
      fun test_upsert/0 %rerun upsert to make sure we can still do our work
    ]
  }].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

test_fetch_collections() ->
  ?OUT("Testing collection names fetch", []),
  [<<"system.indexes">>,<<"test">>] = lists:sort(emongo:get_collections(?POOL)).

test_drop_collection() ->
  ?OUT("Testing collection drop", []),
    ok = emongo:drop_collection(?POOL, "test"),
    false = lists:member(<<"test">>, emongo:get_collections(?POOL)).

test_drop_database() ->
  ?OUT("Testing databsae drop", []),
  ok = emongo:drop_database(?POOL).

test_upsert() ->
  ?OUT("Testing upsert", []),
  Selector = [{<<"_id">>, <<"upsert_test">>}],
  try
    emongo:delete_sync(?POOL, ?COLL, Selector),

    UpsertRes1 = emongo:update_sync(?POOL, ?COLL, Selector,
                                    [{"$set", [{"data", 1}]}], true),
    ?assertEqual(ok, UpsertRes1),
    Find1 = emongo:find_all(?POOL, ?COLL, Selector),
    ?assertEqual([Selector ++ [{<<"data">>, 1}]], Find1),

    UpsertRes2 = emongo:update_sync(?POOL, ?COLL, Selector,
                                    [{"$set", [{"data", 2}]}], true),
    ?assertEqual(ok, UpsertRes2),
    Find2 = emongo:find_all(?POOL, ?COLL, Selector),
    ?assertEqual([Selector ++ [{<<"data">>, 2}]], Find2)
  after
    emongo:delete_sync(?POOL, ?COLL, Selector)
  end,
  ?OUT("Test passed", []).

test_performance() ->
  ?OUT("Testing performance.", []),
%   emongo:delete_sync(?POOL, ?COLL),
  Start = cur_time_ms(),
%   try
%     Ref = make_ref(),
%     start_processes(Ref),
%     block_until_done(Ref)
%   after
%     % Clean up in case something failed.
%     emongo:delete_sync(?POOL, ?COLL)
%   end,
  End = cur_time_ms(),
  ?OUT("Test passed in ~p ms\n", [End - Start]).

start_processes(Ref) ->
  Pid = self(),
  lists:foreach(fun(X) ->
    proc_lib:spawn(fun() ->
      lists:foreach(fun(Y) ->
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
    ok = check_result("insert_sync", IRes, 0),

    [FMRes] = emongo:find_and_modify(?POOL, ?COLL, Selector,
      [{<<"$set">>, [{<<"fm">>, Num}]}], [{new, true}]),
    FMVal = proplists:get_value(<<"value">>, FMRes),
    ?assertEqual(Selector ++ [{<<"fm">>, Num}], FMVal),

    URes = emongo:update_sync(?POOL, ?COLL, Selector,
      [{<<"$set">>, [{<<"us">>, Num}]}], false, [response_options]),
    ok = check_result("update_sync", URes, 1),

    FARes = emongo:find_all(?POOL, ?COLL, Selector),
    ?assertEqual([Selector ++ [{<<"fm">>, Num}, {<<"us">>, Num}]], FARes),

    DRes = emongo:delete_sync(?POOL, ?COLL, Selector, [response_options]),
    ok = check_result("delete_sync", DRes, 1)
  catch _:E ->
    ?OUT("Exception occurred for test ~.16b: ~p\n~p\n",
              [Num, E, erlang:get_stacktrace()]),
    throw(test_failed)
  end.

check_result(Desc,
             {response, _,_,_,_,_, [List]},
             ExpectedN) when is_list(List) ->
  {_, Err} = lists:keyfind(<<"err">>, 1, List),
  {_, N}   = lists:keyfind(<<"n">>,   1, List),
  if Err == undefined, N == ExpectedN -> ok;
  true ->
    ?OUT("Unexpected result for ~p: Err = ~p; N = ~p", [Desc, Err, N]),
    throw({error, invalid_db_response})
  end.

block_until_done(Ref) ->
  lists:foreach(fun(_) ->
    receive {Ref, done} -> ok
    after ?TIMEOUT ->
      ?OUT("No response\n", []),
      throw(test_failed)
    end
  end, lists:seq(1, ?NUM_PROCESSES)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

ensure_started(App) ->
  case application:start(App) of
    ok -> ok;
    {error, {already_started, App}} -> ok
  end.

cur_time_ms() ->
  {MegaSec, Sec, MicroSec} = erlang:now(),
  MegaSec * 1000000000 + Sec * 1000 + erlang:round(MicroSec / 1000).
