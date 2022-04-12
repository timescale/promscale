JSONNET_FMT := jsonnetfmt -n 2 --max-blank-lines 2 --string-style s --comment-style s

default: build

all: fmt lint build clean

fmt:
	find . -name 'vendor' -prune -o -name '*.libsonnet' -print -o -name '*.jsonnet' -print | \
		xargs -n 1 -- $(JSONNET_FMT) -i

lint:
	find . -name 'vendor' -prune -o -name '*.libsonnet' -print -o -name '*.jsonnet' -print | \
		while read f; do \
			$(JSONNET_FMT) "$$f" | diff -u "$$f" -; \
		done

	mixtool lint mixin.libsonnet

build:
	mixtool generate all mixin.libsonnet

clean:
	rm -rf dashboards_out alerts.yaml rules.yaml

test: build
	# If you don't have promtool, install it with
	# go install -a github.com/prometheus/prometheus/cmd/promtool@latest
	promtool check rules alerts.yaml

	# If you don't have gojsontoyaml, install it with
	# go install -a github.com/brancz/gojsontoyaml@latest
	gojsontoyaml -yamltojson < alerts/alerts.yaml | jq -e '.groups[].rules[].annotations | has("runbook_url")' 
