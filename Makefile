PROJECT = sodomongo
PROJECT_DESCRIPTION = New project
PROJECT_VERSION = 0.0.1

COMPILE_FIRST=gen_worker

DEPS =  mongodb folsom folsomite

LOCAL_DEPS = crypto bson

BUILD_DEPS = eredis

dep_mongodb = git https://github.com/dvarkin/mongodb-erlang.git master

SHELL_DEPS = sync

DIALYZER_DIRS = ebin

include erlang.mk

