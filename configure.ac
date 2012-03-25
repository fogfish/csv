dnl
dnl   Copyright (c) 2012, Dmitry Kolesnikov
dnl   All Rights Reserved.
dnl
dnl    Licensed under the 3-clause BSD License (the "License");
dnl    you may not use this file except in compliance with the License.
dnl    You may obtain a copy of the License at
dnl
dnl         http://www.opensource.org/licenses/BSD-3-Clause
dnl
dnl    Unless required by applicable law or agreed to in writing, software
dnl    distributed under the License is distributed on an "AS IS" BASIS,
dnl    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
dnl    See the License for the specific language governing permissions and
dnl    limitations under the License
dnl
AC_INIT([csv], [0.0.1], [dmkolesnikov@gmail.com])
AM_INIT_AUTOMAKE([silent-rules])
AM_SILENT_RULES([yes])
ACX_INIT_GNUWEB

ACX_CHECK_PROG([perl])
ACX_ERLANG

AM_CONFIG_HEADER(config.h)
AC_OUTPUT(
   Makefile      \
   test/Makefile \
)
ACX_DONE

