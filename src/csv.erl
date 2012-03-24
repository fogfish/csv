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
%%    csv file parser
-module(csv).

-export([parse/3, split/4, pparse/4]).

%%
%%
-define(FIELD_BY, $,).
-define(LINE_BY, $\n).

%%
%% parse(In, Fun, App) -> NApp
%%   In  = binary(), input data to parse
%%   Fun = Fun({Event, Line}, App), parser event handler
%%      Event - atom() line | eof
%%      Line  - list() list of fields
%%      App   - internal application state
%%
%% sequentially parses csv file
%%
parse(In, Fun, App) ->
   parse(In, 0, 0, [], Fun, App).

parse(In, Pos, Len, Acc, Fun, App) when Pos + Len < size(In) ->
   case In of
      <<_:Pos/binary, Tkn:Len/binary, ?FIELD_BY,  _/binary>> ->
         % field match
         parse(In, Pos + Len + 1, 0, [Tkn | Acc], Fun, App);
      <<_:Pos/binary, Tkn:Len/binary, ?LINE_BY>> ->
         % last line match
         Fun({line, [Tkn | Acc]}, App);
      <<_:Pos/binary, Tkn:Len/binary, ?LINE_BY, _/binary>> ->
         % line match
         NApp = Fun({line, [Tkn | Acc]}, App),
         parse(In, Pos + Len + 1, 0, [], Fun, NApp);
      _ ->
         % no match increase token
         parse(In, Pos, Len + 1, Acc, Fun, App)
   end;
parse(In, Pos, Len, Acc, Fun, App) ->
   <<_:Pos/binary, Tkn:Len/binary, _/binary>> = In,
   Fun({line, [Tkn | Acc]}, App).
  
%%
%% split(In, Count, Fun, App) -> NApp
%%
%% split csv file on chunks, the count defines number of chunks
split(In, Count, Fun, App) ->
   Size = erlang:round(size(In) / Count), % approximate a shard size
   split(In, 0, Size, Size, Fun, App).
 
split(In, Pos, Size, Size0, Fun, App) when Pos + Size < size(In) ->
   case In of
      <<_:Pos/binary, Shard:Size/binary, ?LINE_BY>> ->
         Fun({shard, Shard}, App);
      <<_:Pos/binary, Shard:Size/binary, ?LINE_BY, _/binary>> ->
         NApp = Fun({shard, Shard}, App),
         split(In, Pos + Size + 1, Size0,    Size0, Fun, NApp);
      _ ->
         split(In, Pos,            Size + 1, Size0, Fun, App)
   end;
split(In, Pos, _Size, _Size0, Fun, App) ->
   <<_:Pos/binary, Shard/binary>> = In,
   Fun({shard, Shard}, App).

%%
%% pparse(In, Count, Fun, App) -> NApp
%%
pparse(In, Count, Fun, App) ->   
   Wrk = fun({shard, Shard}, Id) ->
      Pid = self(),
      spawn(
         fun() ->
            R = parse(Shard, Fun, App),
            Pid ! {shard, Id, R}
         end
      ),
      Id + 1
   end,
   N = split(In, Count, Wrk, 1),
   join(lists:seq(1,N - 1), []).

   
join([H | T], Acc) ->
   receive 
      {shard, H, R} -> join(T, [R|Acc])
   end;
join([], Acc) ->
   Acc.
