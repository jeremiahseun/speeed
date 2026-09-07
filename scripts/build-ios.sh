#!/usr/bin/env bash
# Builds aetherlink-ffi as an XCFramework and emits Swift bindings.
#
# Prerequisites (macOS only):
#   rustup target add aarch64-apple-ios aarch64-apple-ios-sim x86_64-apple-ios
#   Xcode command line tools
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CORE="$ROOT/core"
BUILD="$ROOT/ios/build"
OUT="${1:-$ROOT/ios/AetherLinkCore}"

cd "$CORE"

echo "==> building device and simulator slices"
cargo build --release -p aetherlink-ffi --target aarch64-apple-ios
cargo build --release -p aetherlink-ffi --target aarch64-apple-ios-sim
cargo build --release -p aetherlink-ffi --target x86_64-apple-ios

echo "==> fattening the simulator slice"
mkdir -p "$BUILD/sim"
lipo -create \
  "target/aarch64-apple-ios-sim/release/libaetherlink_ffi.a" \
  "target/x86_64-apple-ios/release/libaetherlink_ffi.a" \
  -output "$BUILD/sim/libaetherlink_ffi.a"

echo "==> generating Swift bindings"
rm -rf "$BUILD/swift"
cargo run --release --bin uniffi-bindgen -- generate \
  --library "target/aarch64-apple-ios/release/libaetherlink_ffi.a" \
  --language swift \
  --out-dir "$BUILD/swift"

# UniFFI emits a modulemap that Xcode needs renamed to module.modulemap inside
# the headers directory of each slice.
mkdir -p "$BUILD/headers"
cp "$BUILD/swift"/*.h "$BUILD/headers/"
cat "$BUILD/swift"/*.modulemap > "$BUILD/headers/module.modulemap"

echo "==> assembling XCFramework"
rm -rf "$OUT.xcframework"
xcodebuild -create-xcframework \
  -library "target/aarch64-apple-ios/release/libaetherlink_ffi.a" -headers "$BUILD/headers" \
  -library "$BUILD/sim/libaetherlink_ffi.a" -headers "$BUILD/headers" \
  -output "$OUT.xcframework"

echo "==> done"
echo "    framework: $OUT.xcframework   (drag into Xcode, embed & sign)"
echo "    swift:     $BUILD/swift/aetherlink.swift   (add to the target)"
