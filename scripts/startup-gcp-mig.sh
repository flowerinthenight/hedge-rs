#!/bin/sh

INTERNAL_IP=$(curl -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/ip)
mkdir -p /etc/hedge/ && echo -n "$INTERNAL_IP" > /etc/hedge/internal-ip

VERSION=$(curl -s https://api.github.com/repos/flowerinthenight/hedge-rs/releases/latest | jq -r ".tag_name")
cd /tmp/ && wget https://github.com/flowerinthenight/hedge-rs/releases/download/$VERSION/hedge-$VERSION-x86_64-linux.tar.gz
tar xvzf hedge-$VERSION-x86_64-linux.tar.gz
cp -v hedge-example /usr/local/bin/hedge
chown root:root /usr/local/bin/hedge

cat >/usr/lib/systemd/system/hedge.service <<EOL
[Unit]
Description=Hedge Example

[Service]
Type=simple
Restart=always
RestartSec=10
ExecStart=/usr/bin/sh -c "RUST_LOG=info /usr/local/bin/hedge projects/p/instances/i/databases/db locktable $(cat /etc/hedge/internal-ip):8080 $(cat /etc/hedge/internal-ip):9090"

[Install]
WantedBy=multi-user.target
EOL

systemctl daemon-reload
systemctl enable hedge
systemctl start hedge
systemctl status hedge
