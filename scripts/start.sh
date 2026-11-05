#!/bin/bash
set -e
cd "$(dirname "$0")/.."
sudo python3 scripts/run_network.py
