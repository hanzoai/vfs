#!/bin/bash
# Every fact about a tree that a file system must keep, and one hash of them:
#
#	treehash.sh DIR FACTS
#
# writes to FACTS one line for each path (type and mode), each file (size and link count, its
# contents' hash), each symbolic link (its target) and each group of paths that are one inode,
# sorted; prints the first 16 hex digits of the hash of FACTS and the number of lines. Two
# installs of one lock file, on a mount and on tmpfs, have the same hash.
set -euo pipefail
dir=${1:?usage: treehash.sh DIR FACTS}
facts=${2:?usage: treehash.sh DIR FACTS}
cd "$dir"
{
	find . -printf 'T %p %y %m\n'
	find . -type f -printf 'S %p %s %n\n'
	find . -type f -print0 | sort -z | xargs -0 -r sha256sum | sed 's/^/H /'
	find . -type l -print0 | sort -z | while IFS= read -r -d '' l; do echo "L $l $(readlink "$l")"; done
	find . -type f -links +1 -printf '%i %p\n' | sort -n | awk '{g[$1]=g[$1]" "$2} END {for (i in g) print "G" g[i]}'
} | LC_ALL=C sort > "$facts"
sha256sum "$facts" | cut -c1-16
wc -l < "$facts"
