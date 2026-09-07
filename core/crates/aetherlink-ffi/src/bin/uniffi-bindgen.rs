//! Binding generator. Invoked by `scripts/build-android.sh` and
//! `scripts/build-ios.sh`; see `docs/STATUS.md` for the handoff steps.
fn main() {
    uniffi::uniffi_bindgen_main()
}
