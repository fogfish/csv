%% @author     Dmitry Kolesnikov, <dmkolesnikov@gmail.com>
%% @copyright  (c) 2012 Dmitry Kolesnikov. All Rights Reserved
%%
%%    Licensed under the 3-clause BSD License (the "License");
%%    you may not use this file except in compliance with the License.
%%    You may obtain a copy of the License at
%%
%%         http://www.opensource.org/licenses/BSD-3-Clause
%%
%%    Unless required by applicable law or agreed to in writing, software
%%    distributed under the License is distributed on an "AS IS" BASIS,
%%    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%    See the License for the specific language governing permissions and
%%    limitations under the License
%%
%% @description
%%    csv file parser example
-module(csv_example).

-export([run/1, extract/2, transform_hash/2, transform_tuple/2, load_ets/2, load_pts/2]).

-define(SET, ["priv/set-300K-8.txt","priv/set-300K-24.txt", "priv/set-300K-40.txt"]).

run(N) ->
   lists:foreach(fun(X) -> extract(X, N) end, ?SET),
   lists:foreach(fun(X) -> transform_hash(X, N) end, ?SET),
   lists:foreach(fun(X) -> transform_tuple(X, N) end, ?SET),
   lists:foreach(fun(X) -> load_ets(X, N) end, ?SET),
   lists:foreach(fun(X) -> load_pts(X, N) end, ?SET).

%%
%% validates performance of extract=parse operation
%% parser event function does not do anything else, except line counting
extract(Filename, N) ->
   %% event function, line counter
   Evt = fun({line, _Line}, X) -> X + 1 end,
   {Tread, {ok, Bin}} = timer:tc(file, read_file, [Filename]),
   {Textr, R}         = timer:tc(csv, pparse, [Bin, N, Evt, 0]),
   L  = lists:foldl(fun(X, A) -> A + X end, 0, R),
   error_logger:info_report([
      extract,
      {file,         Filename},
      {lines,               L},
      {size_MB,     size(Bin) / (1024 * 1024)},
      {read_ms,     Tread / 1000},
      {parse_ms,    Textr / 1000},
      {per_line_us, Textr / L}
   ]).
  
%%
%% validate performance of extract and transform operation
%% parser event function calculates a rolling hash of each parsed line
transform_hash(Filename, N) ->
   %% event function, line counter
   Evt = fun({line, Line}, X) -> erlang:phash2({X, Line}) end,
   {Tread, {ok, Bin}} = timer:tc(file, read_file, [Filename]),
   {Textr, R}         = timer:tc(csv, pparse, [Bin, N, Evt, 0]),
   H  = lists:foldl(fun(X, A) -> erlang:phash2({A, X}) end, 0, R),
   error_logger:info_report([
      {transform,        hash},
      {file,         Filename},
      {hash,               H},
      {size_MB,     size(Bin) / (1024 * 1024)},
      {read_ms,     Tread / 1000},
      {parse_ms,    Textr / 1000},
      {per_line_us, Textr / 300000}
   ]).
   
%%
%% validate performance of extract and transform operation
%% parser event function calculates a rolling hash of each parsed line
transform_tuple(Filename, N) ->
   %% event function, line counter
   Evt = fun({line, Line}, X) -> 
      T = list_to_tuple(lists:reverse(Line)),
      X + 1 
   end,
   {Tread, {ok, Bin}} = timer:tc(file, read_file, [Filename]),
   {Textr, R}         = timer:tc(csv, pparse, [Bin, N, Evt, 0]),
   L  = lists:foldl(fun(X, A) -> A + X end, 0, R),
   error_logger:info_report([
      {transform,       tuple},
      {file,         Filename},
      {size_MB,     size(Bin) / (1024 * 1024)},
      {read_ms,     Tread / 1000},
      {parse_ms,    Textr / 1000},
      {per_line_us, Textr / L}
   ]).   
   
%%
%% parses and imports a csv file, transforms each line into tuple and 
%% load data into ets table
load_ets(Filename, N) ->
   T = list_to_atom(Filename),
   ets:new(T, [public, named_table, {write_concurrency, true}]),
   {Tread, {ok, Bin}} = timer:tc(file, read_file, [Filename]),
   {Textr, R}         = timer:tc(csv, pparse, [
      Bin, N, fun csv_util:import/2, {ets, T}
   ]),
   error_logger:info_report([
      {load,        ets},
      {file,        Filename},
      {size_MB,     size(Bin) / (1024 * 1024)},
      {read_ms,     Tread / 1000},
      {parse_ms,    Textr / 1000},
      {per_line_us, Textr / 300000}
   ]), 
   ets:delete(T).

   
%%
%% parses and imports a csv file, transforms each line into tuple and 
%% load data into process term storage
load_pts(Filename, N) ->
   % spawn process term storage
   Pts = list_to_tuple(
      lists:map(
         fun(_) -> spawn(fun() -> pts([]) end) end,
         lists:seq(1,N)
      )
   ),
   % event function 
   Evt = fun({line, Line}, X) -> 
      T   = list_to_tuple(lists:reverse(Line)),
      Key = (erlang:phash2(erlang:element(1, T)) rem (N - 1)) + 1,
      %io:format('~p~n', [Key]),
      erlang:send(erlang:element(Key, Pts), {put, T}),
      X + 1 
   end,
   {Tread, {ok, Bin}} = timer:tc(file, read_file, [Filename]),
   {Textr, R}         = timer:tc(csv, pparse, [Bin, N, Evt, 0]),
   L  = lists:foldl(fun(X, A) -> A + X end, 0, R),
   error_logger:info_report([
      {load,        pts},
      {file,        Filename},
      {size_MB,     size(Bin) / (1024 * 1024)},
      {read_ms,     Tread / 1000},
      {parse_ms,    Textr / 1000},
      {per_line_us, Textr / L}
   ]),
   lists:map(
      fun(X) -> erlang:send(X, shutdown) end,
      tuple_to_list(Pts)
   ).   
   
   
pts(S) ->
   receive
      {put, T} -> pts([T | S]);
      {get, P} -> P ! S, pts(S);
      shutdown -> ok
   end.
   