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

-export([run/2]).


run(Filename, Wrk) ->
   Cnt = fun(_, X) -> X + 1 end,
   T1 = epoch(),
   {ok, Bin} = file:read_file(Filename),
   T2 = epoch(),
   R  = csv:pparse(Bin, Wrk, Cnt, 0),
   T3 = epoch(),
   L  = lists:foldl(fun(X, A) -> A + X end, 0, R),
   {
      {lines,         L},
      {size,  size(Bin)},
      {read_ms,  (T2 - T1) / 1000},
      {parse_ms, (T3 - T2) / 1000},
      {line_us,  (T3 - T2) / L}
   }.
   
epoch() ->
   {Mega, Sec, Micro} = erlang:now(),
   (Mega * 1000000 + Sec) * 1000000 + Micro.         