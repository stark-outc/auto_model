#!/usr/bin/env bash
#
#********************************************************************
#Date:                  2021-01-20
#FileName：             test.sh
#Copyright (C):        2021 All rights reserved
#********************************************************************
set -x
trim_string() {
    # Usage: trim_string "   example   string    "
        : "${1#"${1%%[![:space:]]*}"}"
	    : "${_%"${_##*[![:space:]]}"}"
	        printf '%s\n' "$_"
		}
name=" john "
trim_string "$name"
for dir in /data/project/fate/python/*/; do
	printf '%s\n' "$dir"
done
