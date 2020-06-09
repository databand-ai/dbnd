#!/usr/bin/env bash

sigquit()
{
   echo "SIGQUIT received"
}

sigint()
{
   echo "SIGINT received"
}

sigterm()
{
    echo "SIGTERM received"
}

sigabort()
{
    echo "SIGABRT received"
}

trap 'sigquit' QUIT
trap 'sigint'  INT
trap 'sigterm' TERM
trap 'sigabort' ABORT

echo "test script started. My PID is $$"
while true; do
  sleep 1
done
