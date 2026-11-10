#!/bin/sh
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
classes="$root/target/test-classes"
rm -rf "$classes"
mkdir -p "$classes"
find "$root/src/main/java" "$root/src/test/java" -name '*.java' -print > "$root/target/sources.list"
javac --release 11 -d "$classes" @"$root/target/sources.list"
java -cp "$classes" com.simplekafka.broker.DayOneChecks
