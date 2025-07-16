#!/bin/bash
servers=(
	10.200.48.119
	10.200.48.146
)

pass='CBLDamo@2025'

for server in "${servers[@]}"; do
(
echo "[$server] == start"
sshpass -p ${pass} ssh -o StrictHostKeyChecking=no root@$server <<EOF

sudo apt install -y sshpass
sudo apt install -y tmux

echo "[$server] == done"
EOF
) &
done

wait
