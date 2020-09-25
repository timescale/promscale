
include dependencies.makefile

PG_CONFIG?=pg_config

EXTENSION=promscale

EXT_VERSION = $(shell cat promscale.control | grep 'default' | sed "s/^.*'\(.*\)'$\/\1/g")

DATA = $(SQL_FILES)
MODULE_big = $(EXTENSION)

OBJS = $(SRCS:.c=.o)
DEPS = $(SRCS:.c=.d)

DEPS += target/release/libpromscale_rs.d

SHLIB_LINK_INTERNAL = target/release/libpromscale_rs.a

MKFILE_PATH := $(abspath $(MAKEFILE_LIST))
CURRENT_DIR = $(dir $(MKFILE_PATH))

TEST_PGPORT ?= 5432
TEST_PGHOST ?= localhost
TEST_PGUSER ?= postgres
TESTS = $(sort $(wildcard test/sql/*.sql))
USE_MODULE_DB=true
REGRESS = $(patsubst test/sql/%.sql,%,$(TESTS))
REGRESS_OPTS = \
	--inputdir=test \
	--outputdir=test \
	--host=$(TEST_PGHOST) \
	--port=$(TEST_PGPORT) \
	--user=$(TEST_PGUSER) \
	--load-language=plpgsql \
	--load-extension=$(EXTENSION)

PGXS := $(shell $(PG_CONFIG) --pgxs)

EXTRA_CLEAN = $(DEPS)

include $(PGXS)
override CFLAGS += -DINCLUDE_PACKAGE_SUPPORT=0 -MMD
override pg_regress_clean_files = test/results/ test/regression.diffs test/regression.out tmp_check/ log/
-include $(DEPS)

all: target/release/libpromscale_rs.a

rust: target/release/libpromscale_rs.a

promscale.so: target/release/libpromscale_rs.a

target/release/libpromscale_rs.a: Cargo.toml Cargo.lock $(RUST_SRCS)
	cargo build --release $(EXTRA_RUST_ARGS)

clean:
	rm -f $(OBJS) $(patsubst %.o,%.bc, $(OBJS))
	rm -f promscale.so
	cargo clean

install: $(SQL_FILES)

.PHONY: all docker-image docker-push rust
