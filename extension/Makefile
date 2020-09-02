
include dependencies.makefile

PG_CONFIG?=pg_config

EXTENSION=timescale_prometheus_extra

EXT_VERSION = $(shell cat timescale_prometheus_extra.control | grep 'default' | sed "s/^.*'\(.*\)'$\/\1/g")

DATA = $(SQL_FILES)
MODULE_big = $(EXTENSION)

OBJS = $(SRCS:.c=.o)
DEPS = $(SRCS:.c=.d)

DEPS += target/release/libtimescale_prometheus_extra_rs.d

SHLIB_LINK_INTERNAL = target/release/libtimescale_prometheus_extra_rs.a

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

all: target/release/libtimescale_prometheus_extra_rs.a

rust: target/release/libtimescale_prometheus_extra_rs.a

timescale_prometheus_extra.so: target/release/libtimescale_prometheus_extra_rs.a

target/release/libtimescale_prometheus_extra_rs.a: Cargo.toml Cargo.lock $(RUST_SRCS)
	cargo build --release $(EXTRA_RUST_ARGS)

clean:
	rm -f $(OBJS) $(patsubst %.o,%.bc, $(OBJS))
	rm timescale_prometheus_extra.so
	cargo clean

install:

package: clean
	@mkdir -p package/lib
	@mkdir -p package/extension
	$(install_sh) -m 755 $(EXTENSION).so 'package/lib/$(EXTENSION).so'
	$(install_sh) -m 644 $(EXTENSION).control 'package/extension/'
	$(install_sh) -m 644 $(SQL_FILES) 'package/extension/'

.PHONY: all docker-image docker-push rust
