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
-module(csv_tests).
-author("Dmitry Kolesnikov <dmkolesnikov@gmail.com>").
-include_lib("eunit/include/eunit.hrl").

-define(CSV, <<"a,b,c\n1,2,3\nd,e,f\n4,5,6\n">>).
-define(QCSV, <<"\"a\",\"b\",\"c\"\n\"1\",\"2\",\"3\"\n">>).


parse_test() ->
   Fun = fun({line, X}, A) -> [lists:reverse(X) | A] end,
   Acc = csv:parse(?CSV, Fun, []),
   ?assert(lists:member([<<"a">>, <<"b">>, <<"c">>], Acc)),
   ?assert(lists:member([<<"1">>, <<"2">>, <<"3">>], Acc)),
   ?assert(lists:member([<<"d">>, <<"e">>, <<"f">>], Acc)),
   ?assert(lists:member([<<"4">>, <<"5">>, <<"6">>], Acc)).
   
parse_quoted_test() ->
   Fun = fun({line, X}, A) -> [lists:reverse(X) | A] end,
   Acc = csv:parse(?QCSV, Fun, []),
   error_logger:error_report([{a, Acc}]),
   ?assert(lists:member([<<"a">>, <<"b">>, <<"c">>], Acc)),
   ?assert(lists:member([<<"1">>, <<"2">>, <<"3">>], Acc)).
   
pparse_test() ->
   Fun = fun({line, X}, A) -> [lists:reverse(X) | A] end,
   Acc = csv:pparse(?CSV, 3, Fun, []),
   ?assert(lists:member([<<"a">>, <<"b">>, <<"c">>], Acc)),
   ?assert(lists:member([<<"1">>, <<"2">>, <<"3">>], Acc)),
   ?assert(lists:member([<<"d">>, <<"e">>, <<"f">>], Acc)),
   ?assert(lists:member([<<"4">>, <<"5">>, <<"6">>], Acc)).
   