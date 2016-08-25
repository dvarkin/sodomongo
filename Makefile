PROJECT = sodomongo
PROJECT_DESCRIPTION = New project
PROJECT_VERSION = 0.0.1

DEPS = mongodb folsom folsomite

LOCAL_DEPS = crypto bson mongodb

SHELL_DEPS = sync

PLT_APPS = sodomongo

DIALYZER_DIRS = src

include erlang.mk
