#!/bin/bash

find /path -mtime +10 -type f |xargs rm -rf