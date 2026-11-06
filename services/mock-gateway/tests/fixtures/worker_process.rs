// 仅用于 worker 生命周期集成测试，不进入生产命令路径。
// 同一个本机二进制扮演父/孙进程，Windows 无需 shell 文件关联。
use std::{env, fs, process::Command, thread, time::Duration};

fn main() {
    let executable = env::current_exe().expect("fixture executable");
    let config = fs::read_to_string(executable.with_extension("config")).expect("fixture config");
    let mut fields = config.split_whitespace();
    let seconds: u64 = fields.next().unwrap().parse().unwrap();
    let tree = fields.next() == Some("true");
    if env::args().nth(1).as_deref() == Some("--grandchild") {
        thread::sleep(Duration::from_secs(seconds));
        return;
    }
    let mut child = tree.then(|| {
        let child = Command::new(&executable)
            .arg("--grandchild")
            .spawn()
            .expect("grandchild");
        let dir = executable.parent().unwrap();
        // 原子发布 PID，读者不会看到尚未写完的标记。
        fs::write(dir.join("grandchild.pid.tmp"), child.id().to_string()).unwrap();
        fs::rename(dir.join("grandchild.pid.tmp"), dir.join("grandchild.pid")).unwrap();
        child
    });
    thread::sleep(Duration::from_secs(seconds));
    if let Some(child) = child.as_mut() {
        child.wait().unwrap();
    }
}
