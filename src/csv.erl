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
%%    The simple CSV-file parser based on event model. The parser generates an
%%    event/callback when the CSV line is parsed. The parser supports both
%%    sequential and parallel parsing.
%%
%%                           Acc
%%                       +--------+
%%                       |        |
%%                       V        |
%%                  +---------+   |
%%    ----Input---->| Parser  |--------> AccN
%%          +       +---------+
%%         Acc0          |
%%                       V
%%                   Event Line 
%%
%%    The parser takes as input binary stream, event handler function and
%%    initial state/accumulator. Event function is evaluated agains current 
%%    accumulator and parsed line of csv-file. Note: The accumaltor allows to
%%    carry-on application specific state throught event functions.
%%
-module(csv).
-author("Dmitry Kolesnikov <dmkolesnikov@gmail.com>").
-export([parse/3, split/4, pparse/4]).

%%
%%
-define(FIELD_BY, $,).
-define(LINE_BY, $\n).

%%
%% parse(In, Fun, Acc0) -> Acc
%%   In  = binary(), input csv data to parse
%%   Fun = fun({line, Line}, Acc0) -> Acc, 
%%      Line  - list() list of parsed fields in reverse order
%%   Acc0 = term() application specific state/term carried throught
%%                 parser event hadlers
%%
%% sequentially parses csv file
%%
parse(In, Fun, Acc0) ->
   parse(In, 0, 0, [], Fun, Acc0).

parse(In, Pos, Len, Line, Fun, Acc0) when Pos + Len < size(In) ->
   case In of
      <<_:Pos/binary, Tkn:Len/binary, ?FIELD_BY,  _/binary>> ->
         % field match
         parse(In, Pos + Len + 1, 0, [Tkn | Line], Fun, Acc0);
      <<_:Pos/binary, Tkn:Len/binary, ?LINE_BY>> ->
         % last line match
         Fun({line, [Tkn | Line]}, Acc0);
      <<_:Pos/binary, Tkn:Len/binary, ?LINE_BY, _/binary>> ->
         % line match
         parse(In, Pos + Len + 1, 0, [], Fun, Fun({line, [Tkn | Line]}, Acc0));
      _ ->
         % no match increase token
         parse(In, Pos, Len + 1, Line, Fun, Acc0)
   end;
parse(In, Pos, Len, Line, Fun, Acc0) ->
   <<_:Pos/binary, Tkn:Len/binary, _/binary>> = In,
   Fun({line, [Tkn | Line]}, Acc0).
  
%%
%% split(In, Count, Fun, Acc0) -> Acc0
%%    In    = binary(), input csv data to split
%%    Count = integer(), number of shard to produce
%%    Fun = fun({shard, Shard}, Acc0) -> Acc, 
%%       Shard  - binary() chunk of csv data
%%    Acc0 = term() application specific state/term carried throught
%%                  parser event hadlers
%%
%% split csv file on chunks
%%
split(In, Count, Fun, Acc0) ->
   Size = erlang:round(size(In) / Count), % approximate a shard size
   split(In, 0, Size, Size, Fun, Acc0).
 
split(In, Pos, Size, Size0, Fun, Acc0) when Pos + Size < size(In) ->
   case In of
      <<_:Pos/binary, Shard:Size/binary, ?LINE_BY>> ->
         Fun({shard, Shard}, Acc0);
      <<_:Pos/binary, Shard:Size/binary, ?LINE_BY, _/binary>> ->
         split(In, Pos + Size + 1, Size0,    Size0, Fun, 
            Fun({shard, Shard}, Acc0)
         );
      _ ->
         split(In, Pos, Size + 1, Size0, Fun, Acc0)
   end;
split(In, Pos, _Size, _Size0, Fun, Acc0) ->
   <<_:Pos/binary, Shard/binary>> = In,
   Fun({shard, Shard}, Acc0).

%%
%% pparse(In, Count, Fun, App) -> NApp
%%   In    = binary(), input csv data to parse
%%   Count = integers(), defines a number of worker processes
%%   Fun   = fun({line, Line}, Acc0) -> Acc, 
%%      Line  - list() list of parsed fields in reverse order
%%   Acc0 = term() application specific state/term carried throught
%%                 parser event hadlers
%%
%% parallel parse csv file, the function shards the input csv data and
%% parses each chunk in own process.
%%
pparse(In, Count, Fun, Acc0) ->   
   Wrk = fun({shard, Shard}, Id) ->
      Pid = self(),
      spawn(
         fun() ->
            R = parse(Shard, Fun, Acc0),
            Pid ! {shard, Id, R}
         end
      ),
      Id + 1
   end,
   N = split(In, Count, Wrk, 1),
   join(lists:seq(1,N - 1), []).

   
join([H | T], Acc) ->
   receive 
      {shard, H, R} when is_list(R) -> join(T, Acc ++ R);
      {shard, H, R} -> join(T, [R|Acc])
   end;
join([], Acc) ->
   Acc.
