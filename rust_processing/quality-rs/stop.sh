#!/bin/bash
mapfile -t servers < server_list.txt

pass='CBLDamo@2025'

for server in "${servers[@]}"; do
(
echo "[$server] == start"
sshpass -p ${pass} ssh -o StrictHostKeyChecking=no root@$server <<EOF

tmux kill-session -t datarig 2>/dev/null

echo "[$server] == done"
EOF
) &
done

wait

tmux kill-session -t datarig 2>/dev/null
