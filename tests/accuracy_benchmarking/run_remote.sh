export PYTHONPATH=$PYTHONPATH:/var/benchmarks/mindsdb:/var/benchmarks/lightwood
cd /var/benchmarks/mindsdb-examples && git pull && cd ../ &&
cd /var/benchmarks/lightwood && git pull --all && git checkout $1 && git pull origin $1 && pip3 install -r requirements.txt &&
cd /var/benchmarks/mindsdb && git pull --all && git checkout $2 && git pull origin $2 && pip3 install -r requirements.txt &&
cd /var/benchmarks/mindsdb/tests/accuracy_benchmarking &&
python3 mindsdb/tests/accuracy_benchmarking/benchmark.py benchmark_cfg.json $3 True;
shutdown 30
