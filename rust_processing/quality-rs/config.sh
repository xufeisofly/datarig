#!/bin/bash
mapfile -t servers < server_list.txt

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
