#!/bin/bash

luarocks install --only-deps /home/app/luastash*.rockspec

tail -f /dev/null