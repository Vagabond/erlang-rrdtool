%%% Copyright 2009 Andrew Thompson <andrew@hijacked.us>. All rights reserved.
%%%
%%% Redistribution and use in source and binary forms, with or without
%%% modification, are permitted provided that the following conditions are met:
%%%
%%%   1. Redistributions of source code must retain the above copyright notice,
%%%      this list of conditions and the following disclaimer.
%%%   2. Redistributions in binary form must reproduce the above copyright
%%%      notice, this list of conditions and the following disclaimer in the
%%%      documentation and/or other materials provided with the distribution.
%%%
%%% THIS SOFTWARE IS PROVIDED BY THE FREEBSD PROJECT ``AS IS'' AND ANY EXPRESS OR
%%% IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
%%% MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO
%%% EVENT SHALL THE FREEBSD PROJECT OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
%%% INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
%%% (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
%%% LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
%%% ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
%%% (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
%%% SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

% @doc An erlang module to interface with rrdtool's remote control mode as an
% erlang port.
-module(rrdtool).

-behaviour(gen_server).

% public API
-export([
		start/0,
		start/1,
		start_link/0,
		start_link/1,
		create/4,
		update/3,
		update/4
]).

% gen_server callbacks
-export([init/1,
		handle_call/3,
		handle_cast/2,
		handle_info/2,
		terminate/2,
		code_change/3
]).

-define(STORE_TYPES,
	['GAUGE', 'COUNTER', 'DERIVE', 'ABSOLUTE', 'COMPUTE']).

% public API

start() ->
	gen_server:start(?MODULE, ["/usr/bin/rrdtool"], []).

start(RRDTool) when is_list(RRDTool) ->
	gen_server:start(?MODULE, [RRDTool], []).

start_link() ->
	gen_server:start_link(?MODULE, ["/usr/bin/rrdtool"], []).

start_link(RRDTool) when is_list(RRDTool) ->
	gen_server:start_link(?MODULE, [RRDTool], []).

create(Pid, Filename, Datastores, RRAs) ->
	gen_server:call(Pid, {create, Filename, format_datastores(Datastores), format_archives(RRAs)}, infinity).

update(Pid, Filename, DatastoreValues) ->
	gen_server:call(Pid, {update, Filename, format_datastore_values(DatastoreValues), n}, infinity).

update(Pid, Filename, DatastoreValues, Time) ->
	gen_server:call(Pid, {update, Filename, format_datastore_values(DatastoreValues), Time}, infinity).

% gen_server callbacks

%% @hidden
init([RRDTool]) ->
	Port = open_port({spawn_executable, RRDTool}, [{line, 1024}, {args, ["-"]}]),
	{ok, Port}.

%% @hidden
handle_call({create, Filename, Datastores, RRAs}, _From, Port) ->
	Command = "create " ++ Filename ++ " " ++ string:join(Datastores, " ") ++ " " ++ string:join(RRAs, " ") ++ "\n",
	io:format("Command: ~p~n", [lists:flatten(Command)]),
	port_command(Port, Command),
	receive
		{Port, {data, {eol, "OK"++_}}} ->
			{reply, ok, Port};
		{Port, {data, {eol, "ERROR:"++Message}}} ->
			{reply, {error, Message}, Port}
	end;
handle_call({update, Filename, {Datastores, Values}, Time}, _From, Port) ->
	Timestamp = case Time of
		n ->
			"N";
		{Megaseconds, Seconds, _Microseconds} ->
			integer_to_list(Megaseconds) ++ integer_to_list(Seconds);
		Other when is_list(Other) ->
			Other
	end,
	Command = ["update ", Filename, " -t ", string:join(Datastores, ":"), " ", Timestamp, ":", string:join(Values, ":"), "\n"],
	io:format("Command: ~p~n", [lists:flatten(Command)]),
	port_command(Port, Command),
	receive
		{Port, {data, {eol, "OK"++_}}} ->
			{reply, ok, Port};
		{Port, {data, {eol, "ERROR:"++Message}}} ->
			{reply, {error, Message}, Port}
	end;
handle_call(Request, _From, State) ->
	{reply, {unknown_call, Request}, State}.

%% @hidden
handle_cast(_Msg, State) ->
	{noreply, State}.

%% @hidden
handle_info(Info, State) ->
	io:format("info: ~p~n", [Info]),
	{noreply, State}.

%% @hidden
terminate(_Reason, _State) ->
	ok.

%% @hidden
code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

% internal functions

format_datastores(Datastores) ->
	format_datastores(Datastores, []).

format_datastores([], Acc) ->
	lists:reverse(Acc);
format_datastores([H | T], Acc) ->
	case H of
		{Name, DST, Arguments} when is_list(Name), is_atom(DST), is_list(Arguments) ->
			case re:run(Name, "^[a-zA-Z0-9_]{1,19}$", [{capture, none}]) of
				nomatch ->
					throw({error, bad_datastore_name, Name});
				match ->
					case lists:member(DST, ?STORE_TYPES) of
						false ->
							throw({error, bad_datastore_type, DST});
						true ->
							format_datastores(T, [["DS:", Name, ":", atom_to_list(DST), ":", format_arguments(DST, Arguments)] | Acc])
					end
			end;
		_ ->
			throw({error, bad_datastore, H})
	end.

format_arguments(DST, Arguments) ->
	case DST of
		'COMPUTE' ->
			% TODO rpn expression validation
			Arguments;
		_ ->
			case Arguments of
				[Heartbeat, Min, Max] when is_integer(Heartbeat), is_integer(Min), is_integer(Max) ->
					io_lib:format("~B:~B:~B", [Heartbeat, Min, Max]);
				[Heartbeat, undefined, undefined] when is_integer(Heartbeat) ->
					io_lib:format("~B:U:U", [Heartbeat]);
				_ ->
					throw({error, bad_datastore_arguments, Arguments})
			end
	end.

format_archives(RRAs) ->
	format_archives(RRAs, []).

format_archives([], Acc) ->
	lists:reverse(Acc);
format_archives([H | T], Acc) ->
	case H of
		{CF, Xff, Steps, Rows} when CF =:= 'MAX'; CF =:= 'MIN'; CF =:= 'AVERAGE'; CF =:= 'LAST' ->
			format_archives(T, [io_lib:format("RRA:~s:~.2f:~B:~B", [CF, Xff, Steps, Rows]) | Acc]);
		_ ->
			throw({error, bad_archive, H})
	end.

format_datastore_values(DSV) ->
	format_datastore_values(DSV, [], []).

format_datastore_values([], TAcc, Acc) ->
	{lists:reverse(TAcc), lists:reverse(Acc)};
format_datastore_values([H | T], TAcc, Acc) ->
	case H of
		{Name, Value} ->
			case re:run(Name, "^[a-zA-Z0-9_]{1,19}$", [{capture, none}]) of
				nomatch ->
					throw({error, bad_datastore_name, Name});
				match ->
					format_datastore_values(T, [Name | TAcc], [value_to_list(Value) | Acc])
			end;
		_ ->
			throw({error, bad_datastore_value, H})
	end.

value_to_list(Value) when is_list(Value) ->
	Value;
value_to_list(Value) when is_integer(Value) ->
	integer_to_list(Value);
value_to_list(Value) when is_float(Value) ->
	float_to_list(Value);
value_to_list(Value) when is_binary(Value) ->
	binary_to_list(Value).
