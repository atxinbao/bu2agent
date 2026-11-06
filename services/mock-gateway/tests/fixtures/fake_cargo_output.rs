// 仅用于 worker 测试解析集成测试，不进入生产命令路径。
// 扮演 `cargo test`：两个套件的 `test result:` 汇总行写到
// stdout，随后往 stderr 写超过 4096 字节的 Cargo 进度，
// 退出码 0。汇总行在完整输出里，不在截断后的 tail 里 ——
// 网关必须解析完整输出，计数才不为 null。
use std::io::Write;

fn main() {
    print!(
        "running 2 tests\ntest a::one ... ok\ntest a::two ... ok\n\ntest result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s\n\nrunning 1 test\ntest b::one ... ok\ntest result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s\n"
    );
    let _ = std::io::stdout().flush();
    let mut err = std::io::stderr().lock();
    for _ in 0..200 {
        let _ =
            writeln!(err, "Running unittests src/lib.rs (target/debug/deps/testpkg-abc)");
    }
    let _ = err.flush();
    std::process::exit(0);
}
