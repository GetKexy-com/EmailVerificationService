#!/bin/bash

set -e  # Exit on error

# 🔹 Define Public IPs - Replace with your actual values
IP1="203.0.113.10"
IP2="203.0.113.11"
NETMASK="255.255.255.248"  # Check with ISP
GATEWAY="203.0.113.1"       # Check with ISP
INTERFACE="eth0"

echo "🚀 Configuring Multiple Public IPs for NestJS on Ubuntu..."

# 🔹 Detect Ubuntu version and configure network
if [ -f "/etc/netplan/*.yaml" ]; then
  echo "🔹 Using Netplan for network configuration..."
  sudo tee /etc/netplan/50-cloud-init.yaml > /dev/null <<EOT
network:
  version: 2
  renderer: networkd
  ethernets:
    $INTERFACE:
      addresses:
        - $IP1/29
        - $IP2/29
      gateway4: $GATEWAY
      nameservers:
        addresses: [8.8.8.8, 8.8.4.4]
EOT
  sudo netplan apply
else
  echo "🔹 Using /etc/network/interfaces..."
  sudo tee -a /etc/network/interfaces > /dev/null <<EOT
auto $INTERFACE
iface $INTERFACE inet static
  address $IP1
  netmask $NETMASK
  gateway $GATEWAY

auto $INTERFACE:1
iface $INTERFACE:1 inet static
  address $IP2
  netmask $NETMASK
EOT
  sudo systemctl restart networking
fi

# 🔹 Configure Routing
echo "🌐 Configuring routing tables..."
echo "200 net1" | sudo tee -a /etc/iproute2/rt_tables
echo "201 net2" | sudo tee -a /etc/iproute2/rt_tables

sudo ip rule add from $IP1 table net1
sudo ip rule add from $IP2 table net2

sudo ip route add default via $GATEWAY dev $INTERFACE table net1
sudo ip route add default via $GATEWAY dev $INTERFACE table net2

# 🔹 Set Up Round-Robin IP Switching
echo "⚡ Setting up round-robin outbound IP rotation..."
sudo iptables -t nat -A POSTROUTING -m statistic --mode nth --every 2 --packet 0 -j SNAT --to-source $IP1
sudo iptables -t nat -A POSTROUTING -m statistic --mode nth --every 2 --packet 1 -j SNAT --to-source $IP2

# 🔹 Save iptables Rules
echo "💾 Saving iptables rules..."
sudo iptables-save | sudo tee /etc/iptables.rules
echo "@reboot iptables-restore < /etc/iptables.rules" | sudo crontab -

# 🔹 Setup NestJS to Listen on Multiple IPs
echo "🛠 DONE!!"
