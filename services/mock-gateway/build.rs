//! macOS link configuration for the EventKit (Swift) dependency.
//!
//! The `eventkit` crate's Swift bridge links the Swift runtime
//! dynamically (`@rpath/libswift_Concurrency.dylib`), but Rust
//! adds no LC_RPATH for the toolchain's Swift library directory.
//! Binaries that retain Swift symbols therefore abort at load
//! time with "Library not loaded". This build script asks the
//! active toolchain for its runtime library paths and adds them
//! as rpaths. macOS only; other platforms do nothing.
//!
//! The path is queried, never hardcoded: the toolchain location
//! differs between machines (Xcode vs Command Line Tools, and
//! CI runners), so a fixed path would break other machines.
//! If the toolchain switches, a clean rebuild picks up the new
//! paths (build scripts do not re-run on toolchain changes).
//!
//! Distribution note: a toolchain rpath fixes dev and CI
//! machines only. A gateway binary shipped to other machines
//! needs its Swift runtime handled separately (bundled dylibs
//! or static linking); that is a later packaging decision.

use std::process::Command;

fn main() {
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() != Ok("macos") {
        return;
    }
    let output = Command::new("xcrun")
        .args(["swiftc", "-print-target-info"])
        .output();
    let output = match output {
        Ok(output) if output.status.success() => output,
        _ => {
            println!("cargo:warning=eventkit link: could not query swiftc target info; Swift runtime rpath not added");
            return;
        }
    };
    let info: serde_json::Value = match serde_json::from_slice(&output.stdout) {
        Ok(info) => info,
        Err(_) => {
            println!("cargo:warning=eventkit link: could not parse swiftc target info; Swift runtime rpath not added");
            return;
        }
    };
    let mut added = false;
    if let Some(paths) = info
        .get("paths")
        .and_then(|paths| paths.get("runtimeLibraryPaths"))
        .and_then(|paths| paths.as_array())
    {
        for path in paths.iter().filter_map(|path| path.as_str()) {
            println!("cargo:rustc-link-arg=-Wl,-rpath,{path}");
            added = true;
        }
    }
    if !added {
        println!("cargo:warning=eventkit link: no Swift runtime library paths found; Swift runtime rpath not added");
    }
}
