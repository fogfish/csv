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

-export([parse/2, import/2]).

%%
%% parses a file, event function does nothing exept line counter
parse(Filename, Wrk) ->
   Cnt = fun({line, _}, X) -> X + 1 end,
   T1 = epoch(),
   {ok, Bin} = file:read_file(Filename),
   T2 = epoch(),
   R  = csv:pparse(Bin, Wrk, Cnt, 0),
   T3 = epoch(),
   L  = lists:foldl(fun(X, A) -> A + X end, 0, R),
   io:format("lines: ~b~nsize (MB): ~f~nread (ms): ~f~nparse (ms): ~f~nper line (us): ~f~n", [L, (size(Bin) / (1024 * 1024)), ((T2 - T1) / 1000), ((T3 - T2) / 1000), ((T3 - T2) / L)]).
   
%%
%% parses and imports a csv file into ets table
import(Filename, Wrk) ->
   Ets = ets:new(noname, [public, ordered_set, {write_concurrency, true}]),
   T1  = epoch(),
   {ok, Bin} = file:read_file(Filename),
   T2  = epoch(),
   csv:pparse(Bin, Wrk, fun csv_util:import/2, {ets, Ets}),
   T3  = epoch(),
   io:format("size (MB): ~f~nread (ms): ~f~nparse (ms): ~f~n", [(size(Bin) / (1024 * 1024)), ((T2 - T1) / 1000), ((T3 - T2) / 1000)]),
   Ets.
   
   
epoch() ->
   {Mega, Sec, Micro} = erlang:now(),
   (Mega * 1000000 + Sec) * 1000000 + Micro.         