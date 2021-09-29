echo 'unzipping bench dataset'
# We zip bench dataset with `zip -r -9 bench_data.zip bench_data`
unzip pkg/tests/testdata/bench_data.zip -d pkg/tests/testdata
go test ./pkg/tests/benchmarks/integration -benchtime=100x -bench=.
echo 'cleaning up generated datasets'
rm -R pkg/tests/testdata/bench_data
echo 'done'