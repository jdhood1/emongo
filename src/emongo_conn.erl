%% Copyright (c) 2009 Jacob Vorreuter <jacob.vorreuter@gmail.com>
%%
%% Permission is hereby granted, free of charge, to any person
%% obtaining a copy of this software and associated documentation
%% files (the "Software"), to deal in the Software without
%% restriction, including without limitation the rights to use,
%% copy, modify, merge, publish, distribute, sublicense, and/or sell
%% copies of the Software, and to permit persons to whom the
%% Software is furnished to do so, subject to the following
%% conditions:
%%
%% The above copyright notice and this permission notice shall be
%% included in all copies or substantial portions of the Software.
%%
%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
%% EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
%% OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
%% NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
%% HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
%% WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
%% FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
%% OTHER DEALINGS IN THE SOFTWARE.
-module(emongo_conn).
-include("emongo.hrl").
-export([start_link/4, stop/1, send/4, send_recv/4, queue_lengths/1, write_pid/1]).
-export([init_loop/4]).

-record(state, {
  pool_id,
  socket,
  host,
  port,
  socket_options,
  dict = dict:new(),
  socket_data = <<>>,
  timeout_count = 0
}).

start_link(PoolId, Host, Port, SocketOptions) ->
  Args = [PoolId, Host, Port, SocketOptions],
  {ok, _} = proc_lib:start_link(?MODULE, init_loop, Args, ?CONN_TIMEOUT).

stop(Pid) ->
  Pid ! emongo_conn_close.

send(Pid, ReqId, Packet, Timeout) ->
  gen_call(Pid, emongo_conn_send, ReqId, {ReqId, Packet}, Timeout).

send_recv(Pid, ReqId, Packet, Timeout) ->
  Resp = gen_call(Pid, emongo_conn_send_recv, ReqId, {ReqId, Packet}, Timeout),
  Documents = emongo_bson:decode(Resp#response.documents),
  Resp#response{documents = Documents}.

queue_lengths(Pid) ->
  case erlang:process_info(Pid, message_queue_len) of
    {_, QueueLen} -> QueueLen;
    _             -> 0
  end.

write_pid(Pid) -> Pid.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init_loop(PoolId, Host, Port, SocketOptions) ->
  Socket = open_socket(Host, Port, SocketOptions),
  ok = proc_lib:init_ack({ok, self()}),
  loop(#state{
    pool_id        = PoolId,
    socket         = Socket,
    host           = Host,
    port           = Port,
    socket_options = SocketOptions
  }).

loop(State = #state{pool_id = PoolId, socket = Socket, dict = Dict}) ->
  MaxPipelineDepth = emongo:get_config(PoolId, max_pipeline_depth),
  CanSend = (MaxPipelineDepth == 0) or (dict:size(Dict) < MaxPipelineDepth),
  NewState = try
    _NewState = receive
      % Calls to 'receive' will preserve the order of the message queue.  Calls to receive will not prioritize messages
      % that match clauses below in the order of the clauses.  It will match them in the order of the messages in the
      % queue:
      %   1> Pid = self().
      %   <0.33.0>
      %   2> Pid ! a.
      %   a
      %   3> Pid ! b.
      %   b
      %   4> receive b -> b; a -> a end.
      %   a
      %   5> receive b -> b; a -> a end.
      %   b
      % FromRef = {From, Mref}
      {emongo_conn_send, FromRef, {_ReqId, Packet}} when CanSend ->
        ok = gen_tcp:send(Socket, Packet),
        gen:reply(FromRef, ok),
        State;
      {emongo_conn_send_recv, FromRef, {ReqId, Packet}} when CanSend ->
        ok = gen_tcp:send(Socket, Packet),
        State#state{dict = dict:append(ReqId, FromRef, Dict)};
      {tcp, _Socket, NewData} ->
        ProcState = process_bin(State#state{socket_data = <<(State#state.socket_data)/binary, NewData/binary>>}),
        % We are receiving data on this socket, so clear timeout_count.
        ProcState#state{timeout_count = 0};
      {emongo_recv_timeout, FromRef, ReqId} ->
        gen:reply(FromRef, ok),
        % If the message related to this request is still in the mailbox waiting to be sent (when CanSend is true), go
        % ahead and clear it out (without regard for how CanSend is set).
        receive
          {emongo_conn_send,      _FromRef, {ReqId, _}} -> ok;
          {emongo_conn_send_recv, _FromRef, {ReqId, _}} -> ok
        after 0 -> ok
        end,
        NewTimeoutCount = State#state.timeout_count + 1,
        case NewTimeoutCount > emongo:get_config(PoolId, disconnect_timeouts) of
          true -> exit(emongo_too_many_timeouts);
          _    -> State#state{dict = dict:erase(ReqId, Dict), timeout_count = NewTimeoutCount}
        end;
      {tcp_closed, _Socket}        -> exit(emongo_tcp_closed);
      {tcp_error, _Socket, Reason} -> exit({emongo, Reason});
      emongo_listen_exited         -> exit(emongo_listen_exited);
      emongo_conn_close            -> exit(emongo_conn_close)
    end
  catch Class:Error ->
    gen_tcp:close(Socket),
    % The Pids waiting for responses in Dict will get errors when this Pid exits.  They don't have to wait for a
    % timeout.
    % Throw a meaningful error that the emongo module can handle for connections that exit.
    case Error of
      emongo_conn_close        -> exit(shutdown);
      emongo_too_many_timeouts -> exit(normal);
      _                        ->
        ?EXCEPTION("Exiting: ~p", [Error]),
        erlang:raise(Class, Error, erlang:get_stacktrace())
    end
  end,
  loop(NewState).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

open_socket(Host, Port, SocketOptions) ->
  BufferSize = proplists:get_value(buffer, SocketOptions, 1048576), % 1 MB: MongoDB 3.4+ sends up to 16 MB at a time.
  BufferOptions = case BufferSize of
    0 -> [];
    _ -> [{sndbuf, BufferSize}, {recbuf, BufferSize}, {buffer, BufferSize}]
  end,
  % {exit_on_close, true}
  % TODO: We should probably use {active, once} and reset it when we receive each piece of data.
  Options = [binary, {active, true}, {keepalive, true} | lists:keydelete(buffer, 1, SocketOptions) ++ BufferOptions],
  case gen_tcp:connect(Host, Port, Options) of
    {ok, Sock}      -> Sock;
    {error, Reason} -> exit({emongo_failed_to_open_socket, Reason})
  end.

gen_call(Pid, Label, ReqId, Request, Timeout) ->
  try
    case gen:call(Pid, Label, Request, Timeout) of
      {ok, Result} -> Result;
      Error        -> exit(Error)
    end
  catch
    exit:timeout ->
      % TODO: If the response to the gen:call() above comes back right in this gap (i.e. before the request has been
      % cleared from the connection Pid's dictionary), the reply could be sent to this Pid's mailbox and never cleaned
      % up.
      try
        % Tell the connection Pid that this call is timing out.
        gen:call(Pid, emongo_recv_timeout, ReqId, Timeout)
      catch
        % If a timeout occurred while trying to communicate with the connection, something is really backed up.
        % However, if this happens after a connection goes down, it's expected.
        exit:timeout -> exit({emongo_conn_error, overloaded});
        % Any other error should not override the timeout we are already handling.
        _:E -> E
      end,
      exit({emongo_conn_error, timeout});
    _:_ ->
      % If the connection Pid above exits before responding to this Pid, the gen:call() function above will exit with
      % the exit status of the connection Pid.  For example, consider the following:
      % try
      %   gen:call(proc_lib:spawn(fun() ->
      %     timer:sleep(100),
      %     exit(emongo_tcp_closed)
      %   end), test, {1, asdf}, 200)
      % catch C:E ->
      %   {C, E}
      % end.
      % That code returns: {exit,emongo_tcp_closed}
      exit({emongo_conn_error, connection_closed})
  end.

process_bin(State = #state{dict = Dict, socket_data = Data}) ->
  case emongo_packet:decode_response(Data) of
    undefined -> State;
    {Resp = #response{header = #header{response_to = ResponseTo}}, Tail} ->
      StateDict = try
        [FromRef] = dict:fetch(ResponseTo, Dict),
        gen:reply(FromRef, Resp),
        State#state{dict = dict:erase(ResponseTo, Dict)}
      catch _:badarg ->
        % The request must have timed out.
        State
      end,
      % Continue processing Tail in case there's another complete message in it.
      process_bin(StateDict#state{socket_data = Tail})
  end.
