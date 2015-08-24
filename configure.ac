AC_INIT([pinba_engine], [1.2.0-dev])
AC_CONFIG_MACRO_DIR([m4])
AM_CONFIG_HEADER(src/config.h)
AX_PREFIX_CONFIG_H([src/pinba_config.h])
AM_INIT_AUTOMAKE

AC_CONFIG_SUBDIRS([sparsehash])

AC_PROG_CC
AC_PROG_CXX
AC_PROG_CXXCPP

AC_PROG_LIBTOOL
LIBTOOL="$LIBTOOL --preserve-dup-deps"
AC_SUBST(LIBTOOL)

AC_SUBST(MYSQL_INC)

AC_C_CONST
AC_TYPE_SIZE_T
AC_CHECK_HEADERS(limits.h syslimits.h string.h strings.h unistd.h stdint.h)
AC_PROG_SED
AC_PROG_AWK

AC_CHECK_FUNCS([strndup sysconf recvmmsg])

changequote({,})
CXXFLAGS=`echo "$CXXFLAGS" | sed -e 's/ -DMYSQL_DYNAMIC_PLUGIN -DNDEBUG//g'`
CFLAGS=`echo "$CFLAGS" | sed -e 's/ -DNDEBUG//g'`
changequote([,])
CXXFLAGS="$CXXFLAGS -DMYSQL_DYNAMIC_PLUGIN -DNDEBUG"
CFLAGS="$CFLAGS -DNDEBUG"

AX_CONFIG_NICE([config.nice])

dnl check for floor and libm
AC_CHECK_LIB([m], [floor], [LIBS="$LIBS -lm"], [AC_MSG_ERROR([can't continue without libm])])

AC_CHECK_LIB([pthread], [pthread_setaffinity_np], [
  AC_DEFINE([HAVE_PTHREAD_SETAFFINITY_NP], [1], [Whether pthread_setaffinity_np() is available])
], [AC_MSG_NOTICE([can't find pthread_setaffinity_np()])])

STANDARD_PREFIXES="/usr /usr/local /opt /local"

dnl autorevision {{{
AUTOREVISION="$srcdir/autorevision.sh"

v=$($AUTOREVISION -t sh)
if test "$?" = "0"; then
  eval "$v"

  AC_DEFINE_UNQUOTED([VCS_DATE], "$VCS_DATE", [vcs date])
  AC_DEFINE_UNQUOTED([VCS_BRANCH], "$VCS_BRANCH", [vcs branch])
  AC_DEFINE_UNQUOTED([VCS_FULL_HASH], "$VCS_FULL_HASH", [vcs full hash])
  AC_DEFINE_UNQUOTED([VCS_SHORT_HASH], "$VCS_SHORT_HASH", [vcs short hash])
  AC_DEFINE_UNQUOTED([VCS_WC_MODIFIED], "$VCS_WC_MODIFIED", [vcs wc modified])
fi
dnl }}}

dir_resolve() dnl {{{
{
  pwd=`pwd`
  cd "$1" 2>/dev/null || cd "${pwd}/${1}" 2>/dev/null
  if test "$?" = "0"; then
    echo `pwd -P`
  else
    echo "$1"
  fi
}
dnl }}}

dnl {{{ --with-libdir
AC_ARG_WITH(libdir,
  [AS_HELP_STRING([--with-libdir],[look for libraries in .../NAME rather than .../lib])
  ],
  [LIBDIR=$with_libdir],
  [LIBDIR=lib]
)
dnl }}}

dnl {{{ --disable-rpath
AC_ARG_ENABLE(rpath,
  [AS_HELP_STRING([--disable-rpath],[disable passing additional runtime library search paths])
  ],
  [PINBA_RPATH=no],
  [PINBA_RPATH=yes]
)
dnl }}}

dnl {{{ check for rpath support
AC_MSG_CHECKING([if compiler supports -R])
AC_CACHE_VAL(pinba_cv_cc_dashr,[
  SAVE_LIBS=$LIBS
  LIBS="-R /usr/$LIBDIR $LIBS"
  AC_TRY_LINK([], [], pinba_cv_cc_dashr=yes, pinba_cv_cc_dashr=no)
  LIBS=$SAVE_LIBS])
AC_MSG_RESULT([$pinba_cv_cc_dashr])
if test $pinba_cv_cc_dashr = "yes"; then
  ld_runpath_switch=-R
else
  AC_MSG_CHECKING([if compiler supports -Wl,-rpath,])
  AC_CACHE_VAL(pinba_cv_cc_rpath,[
    SAVE_LIBS=$LIBS
    LIBS="-Wl,-rpath,/usr/$LIBDIR $LIBS"
    AC_TRY_LINK([], [], pinba_cv_cc_rpath=yes, pinba_cv_cc_rpath=no)
    LIBS=$SAVE_LIBS])
  AC_MSG_RESULT([$pinba_cv_cc_rpath])
  if test $pinba_cv_cc_rpath = "yes"; then
    ld_runpath_switch=-Wl,-rpath,
  else
    ld_runpath_switch=-L
  fi
fi
if test "$PINBA_RPATH" = "no"; then
  ld_runpath_switch=
fi
dnl }}}

dnl {{{ --with-mysql
AC_MSG_CHECKING(for MySQL source code)
AC_ARG_WITH(mysql,
[AS_HELP_STRING([--with-mysql],[specify MySQL sources directory])
],
[
],
[
  AC_MSG_ERROR([Please provide path to the MySQL sources directory])
])

if test "x$with_mysql" = "xno"; then
  AC_MSG_ERROR([can't continue without MySQL sources])
else

  if test "x$with_mysql" = "xyes"; then
    AC_MSG_ERROR([sorry, I'm not that smart to guess where the MySQL sources are, please specify the path])
  fi

  with_mysql=`dir_resolve "$with_mysql"`

  HEADERS="include/my_dir.h include/mysql/plugin.h include/mysql.h include/mysql_version.h"
  for file in $HEADERS; do
    if ! test -r "$with_mysql/$file"; then
      AC_MSG_ERROR([Failed to find required header file $file in $with_mysql, check the path and make sure you've run './configure ..<options>.. && cd include && make' in MySQL sources dir])
    fi
  done

  AC_MSG_RESULT([$with_mysql])

  CFLAGS_old="$CFLAGS"
  CFLAGS="-I$with_mysql/include $CFLAGS"

  AC_MSG_CHECKING([for MySQL version >= 5.1])
  AC_TRY_RUN([
#include <stdlib.h>
#include <mysql_version.h>
main() {
#if MYSQL_VERSION_ID >= 50100
  exit(0);
#else
  exit(1);
#endif
}
  ], [
    AC_MSG_RESULT([ok])
  ], [
    AC_MSG_ERROR([MySQL 5.1+ is required])
  ])

  dnl check for 5.5 version with missing files and broken method signatures
  MISSING_HEADER="sql/mysql_priv.h"
  if ! test -r "$with_mysql/$MISSING_HEADER"; then
    AC_DEFINE([MYSQL_VERSION_5_5], [1], [Whether we have MySQL 5.5])
  fi

  CFLAGS="$CFLAGS_old"

  AC_DEFINE([MYSQL_SRC], [1], [Source directory for MySQL])
  MYSQL_INC="-I$with_mysql/sql -I$with_mysql/include -I$with_mysql/regex -I$with_mysql"
fi


dnl }}}

dnl {{{ --enable-static-build
AC_ARG_ENABLE(static-build,
  [AS_HELP_STRING([--enable-static-build],[link against non-system libs statically])
  ],
  [
    AC_MSG_CHECKING([if static linking is forced])
    if test x"$enableval" = xyes ; then
      FORCE_STATIC_BUILD=yes
    else
      FORCE_STATIC_BUILD=no
    fi
    AC_MSG_RESULT([$FORCE_STATIC_BUILD])
  ]
)
dnl }}}

dnl {{{ --with-judy
AC_ARG_WITH(judy,
  [AS_HELP_STRING([--with-judy],[specify Judy install prefix])
  ],
  [ ],
  [with_judy=yes]
)

if test "x$with_judy" = "xno"; then
  AC_MSG_ERROR([can't continue without Judy])
else
  AC_MSG_CHECKING([Judy install prefix])

  if test "x$with_judy" = "xyes"; then
    for i in `echo "$STANDARD_PREFIXES"`; do
      if test -f "$i/include/Judy.h"; then
        JUDY_DIR="$i"
        break;
      fi
    done
  else

    with_judy=`dir_resolve "$with_judy"`

    if test -f "$with_judy/include/Judy.h"; then
      JUDY_DIR="$with_judy"
    else
      AC_MSG_ERROR([Can't find Judy headers under $with_judy directory]);
    fi
  fi

  if test "x$JUDY_DIR" = "x"; then
    AC_MSG_ERROR([Unable to locate Judy headers, please use --with-judy=<DIR>]);
  fi

  AC_MSG_RESULT([$JUDY_DIR])

  CXXFLAGS="$CXXFLAGS -I$JUDY_DIR/include"
  CFLAGS="$CFLAGS -I$JUDY_DIR/include"

  if test "$FORCE_STATIC_BUILD" = "yes"; then
    LIBS="$LIBS $JUDY_DIR/$LIBDIR/libJudy.a"
  else
    LDFLAGS="$LDFLAGS -L$JUDY_DIR/$LIBDIR"
    LIBS="$LIBS -lJudy"

    if test "$PINBA_RPATH" != "no"; then
      LDFLAGS="$LDFLAGS $ld_runpath_switch$JUDY_DIR/$LIBDIR"
    fi
  fi
fi
dnl }}}

dnl {{{ --enable-debug
AC_ARG_ENABLE(debug,
  [AS_HELP_STRING([--enable-debug],[enable debugging symbols and compile flags])
  ],
  [
    if test x"$enableval" = xyes ; then
      debug="yes"
    else
      debug="no"
    fi
  ]
)

if test x"$debug" = xyes ; then
  DEBUG_FLAGS="-DSAFE_MUTEX -DDBUG_ON -DEXTRA_DEBUG -DUNIV_MUST_NOT_INLINE -DFORCE_INIT_OF_VARS -DPINBA_DEBUG"

  AC_DEFINE([DEBUG_ON], [1], [debug is on])

  if test x"$GCC" = xyes; then

    dnl Remove any optimization flags from CFLAGS
    changequote({,})
    CXXFLAGS=`echo "$CXXFLAGS" | sed -e 's/-O[0-9s]*//g'`
    CXXFLAGS=`echo "$CXXFLAGS" | sed -e 's/-g[0-2]\? //g'`
    CFLAGS=`echo "$CFLAGS" | sed -e 's/-O[0-9s]*//g'`
    CFLAGS=`echo "$CFLAGS" | sed -e 's/-g[0-2]\? //g'`
    changequote([,])
    CXXFLAGS="$CXXFLAGS -g3 -Wall -O0 $DEBUG_FLAGS"
    CFLAGS="$CFLAGS -g3 -Wall -O0 $DEBUG_FLAGS"
  fi

  ADD_FLAGS=""

  dnl Do not strip symbols from developer object files.
  INSTALL_STRIP_FLAG=""
else
  AC_DEFINE([DEBUG_OFF], [1], [debug is off])
  ADD_FLAGS="-DJUDYERROR_NOTEST"

  dnl Make sure to strip symbols from non-developer object files.
  INSTALL_STRIP_FLAG="-s"
fi
dnl }}}

AC_SUBST(INSTALL_STRIP_FLAG)

AC_SUBST(DEPS_LIBS)
AC_SUBST(DEPS_CFLAGS)

AC_OUTPUT(Makefile src/Makefile)
