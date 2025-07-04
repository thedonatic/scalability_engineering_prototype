#!/bin/sh
export NODE_ID=$(hostname)
export NODE_ADDR="http://$(hostname):5000"
exec python app.py