PROJECT = sodomongo
PROJECT_DESCRIPTION = New project
PROJECT_VERSION = 0.0.1

COMPILE_FIRST=gen_worker

DEPS = mongodb folsom folsomite

LOCAL_DEPS = crypto bson mongodb

dep_mongodb = git https://github.com/dvarkin/mongodb-erlang.git master

SHELL_DEPS = sync

PLT_APPS = sodomongo

DIALYZER_DIRS = src

include erlang.mk
