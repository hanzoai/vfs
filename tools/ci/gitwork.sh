#!/bin/bash
# The git workload of a coding run, for the tree hashes it must give on a mounted Volume and on
# tmpfs alike:
#
#	gitwork.sh DIR ORIGIN
#
# In DIR (a directory on the file system under test) it clones the repository at ORIGIN (a
# path) with depth 1, edits, adds, commits, branches, moves a file, checks out, collects garbage
# and checks the repository, and prints what must not depend on the file system: the tree
# hashes after each step, how many files and of what mode and size the work tree has, and a hash
# of their contents. Run it on the mount and on tmpfs and compare the two outputs.
set -euo pipefail
dir=${1:?usage: gitwork.sh DIR ORIGIN}
origin=${2:?usage: gitwork.sh DIR ORIGIN}
export HOME=$dir/home GIT_CONFIG_NOSYSTEM=1
export GIT_AUTHOR_NAME=t GIT_AUTHOR_EMAIL=t@t GIT_COMMITTER_NAME=t GIT_COMMITTER_EMAIL=t@t
export GIT_AUTHOR_DATE="2024-01-01T00:00:00Z" GIT_COMMITTER_DATE="2024-01-01T00:00:00Z"
mkdir -p "$HOME"
cd "$dir"
git clone -q --depth 1 "file://$origin" w
cd w
git config core.untrackedCache true
git config gc.auto 0
echo "untracked after clone: $(git status --porcelain | wc -l)"
echo hello > NEWFILE
mkdir -p sub/deep
echo x > sub/y
chmod +x sub/y
ln -s NEWFILE link
printf 'a\nb\n' > sub/deep/z
git add -A
git commit -q -m c1
echo "tree after the first commit: $(git rev-parse 'HEAD^{tree}')"
git checkout -q -b br
echo more >> NEWFILE
rm sub/y
git mv sub/deep/z sub/zz
git add -A
git commit -q -m c2
echo "tree on the branch: $(git rev-parse 'HEAD^{tree}')"
git checkout -q -
echo "tree back: $(git rev-parse 'HEAD^{tree}'), untracked: $(git status --porcelain | wc -l)"
git checkout -q br
echo "untracked on the branch: $(git status --porcelain | wc -l)"
git gc -q
git fsck --full 2>&1 | tail -3
echo "tree after gc: $(git rev-parse 'HEAD^{tree}')"
echo "files: $(find . -path ./.git -prune -o \( -type f -o -type l \) -print | wc -l)"
echo "contents: $(find . -path ./.git -prune -o -type f -print0 | sort -z | xargs -0 sha256sum | sha256sum | cut -c1-16)"
echo "modes: $(find . -path ./.git -prune -o -printf '%p %m %y\n' | sort | sha256sum | cut -c1-16)"
echo "sizes: $(find . -path ./.git -prune -o \( -type f -o -type l \) -printf '%p %s\n' | sort | sha256sum | cut -c1-16)"
git count-objects -v | tr '\n' ' '
echo
