#!/bin/bash

server0=10.200.48.109
servers=(
	10.200.48.119
	10.200.48.146
)

pass='CBLDamo@2025'

cargo build --release

# 拼接脚本参数为一个字符串（正确地保留引号）
args="$*"

for server in "${servers[@]}"; do
(
echo "[$server] == start"
sshpass -p ${pass} ssh -o StrictHostKeyChecking=no root@$server <<EOF

sshpass -p '${pass}' scp -o StrictHostKeyChecking=no root@"${server0}":/root/dataprocess/dclm/rust_processing/quality-rs/target/release/quality-rs /root/

cd /root

cat > /root/.env <<EOF2
OSS_ACCESS_KEY_ID=D3XdW8uT66mdJ3TO
OSS_ACCESS_KEY_SECRET=LpBPv841EkOhqNV8tiaknQfWJJm1uP
REDIS_HOST=10.200.48.152
REDIS_PORT=6380
EOF2

tmux kill-session -t datarig 2>/dev/null

tmux new-session -d -s datarig "set -a && source /root/.env && ./quality-rs ${args}"

echo "[$server] == done"
EOF
) &
done

wait

tmux kill-session -t datarig 2>/dev/null
tmux new-session -d -s datarig "cd target/release && ./quality-rs ${args}"
