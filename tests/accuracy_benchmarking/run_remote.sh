cd /var/benchmarks &&
export PYTHONPATH=$PYTHONPATH:/var/benchmarks/mindsdb:/var/benchmarks/lightwood
cd mindsdb_examples && git pull && cd ../ &&
cd lightwood && git pull --all && git checkout $1 && git pull origin $1 && pip3 install -r requirements.txt && cd ../ &&
cd mindsdb && git pull --all && git checkout $2 && git pull origin $2 && pip3 install -r requirements.txt && cd ../ &&
cd mindsdb/tests/accuracy_benchmarking &&
python3 mindsdb/tests/accuracy_benchmarking/benchmark.py benchmark_cfg.json $3 True;
shutdown 30
