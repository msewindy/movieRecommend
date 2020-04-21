#!/usr/bin/env bash
source /etc/profile
pio build
pio train |tee train.log
nohup pio deploy --port &