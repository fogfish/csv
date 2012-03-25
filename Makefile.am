#
#   Copyright (c) 2012, Dmitry Kolesnikov
#   All Rights Reserved.
#
#    Licensed under the 3-clause BSD License (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#         http://www.opensource.org/licenses/BSD-3-Clause
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License
#
SUBDIRS = test

csv_SRC = \
   src/csv.erl \
   src/csv_util.erl 

nobase_pkgliberl_SCRIPTS = \
   ebin/csv.app \
   $(csv_BEAM)

SET = \
   priv/set-300K-8.txt \
   priv/set-300K-24.txt \
   priv/set-300K-40.txt

CLEANFILES = $(nobase_pkgliberl_SCRIPTS) $(SET)
EXTRA_DIST = $(csv_SRC)

example: priv/csv_example.beam $(SET)

priv/set-%:
	$(AM_V_GEN)n=`echo "$@" | sed -n -e 's|.*-\([0-9]*\)K-.*|\1|p'`; \
	t=`echo "$@" | sed -n -e 's|.*-[0-9]*K-\(.*\)\..*|\1|p'`; \
	$(PERL) priv/gen_set.pl $$n $$t > $@

@MAKE_GNUWEB@

