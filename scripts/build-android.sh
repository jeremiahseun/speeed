#!/usr/bin/env bash
# Builds aetherlink-ffi for Android and emits Kotlin bindings.
#
# Prerequisites (local machine, not the cloud container):
#   rustup target add aarch64-linux-android armv7-linux-androideabi x86_64-linux-android
#   cargo install cargo-ndk
#   ANDROID_NDK_HOME set (Android Studio → SDK Manager → NDK)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CORE="$ROOT/core"
OUT="${1:-$ROOT/android/aetherlink/src/main}"

: "${ANDROID_NDK_HOME:?set ANDROID_NDK_HOME to your NDK install}"

echo "==> building native libraries"
cd "$CORE"
cargo ndk \
  -t arm64-v8a \
  -t armeabi-v7a \
  -t x86_64 \
  -o "$OUT/jniLibs" \
  build --release -p aetherlink-ffi

echo "==> generating Kotlin bindings"
# Library mode reads the metadata out of the built .so, so the bindings can
# never drift from the Rust that produced them.
cargo run --release --bin uniffi-bindgen -- generate \
  --library "$OUT/jniLibs/arm64-v8a/libaetherlink_ffi.so" \
  --language kotlin \
  --out-dir "$OUT/java"

echo "==> done"
echo "    jniLibs:  $OUT/jniLibs"
echo "    bindings: $OUT/java/uniffi/aetherlink/"
