tmux new-session -d -s benchmark;
tmux send-keys 'chmod +x /var/benchmarks/mindsdb/tests/accuracy_benchmarking/run_remote.sh; bash /var/benchmarks/mindsdb/tests/accuracy_benchmarking/run_remote.sh "$1" "$2" "$3"; tmux kill-session benchmark' C-m;
tmux detach -s test;
