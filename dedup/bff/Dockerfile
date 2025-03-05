# 使用 Ubuntu 22.04.5 LTS 作为基础镜像
FROM ubuntu:22.04

# 切换时区交互提示，避免一些安装中的时区询问
ENV DEBIAN_FRONTEND=noninteractive

# 1. 更新系统并安装必要依赖
RUN apt-get update && \
    apt-get install -y libssl-dev && \
    rm -rf /var/lib/apt/lists/*

# 2. 设置环境变量: 替换 Rustup 源（USTC、TUNA等）
#    因为 Docker 的 RUN 是在新 shell 中执行，不能直接用 '>> ~/.bashrc' 的方式
#    这里我们可以使用 ENV 或 RUN echo >> /root/.bashrc
RUN echo "export RUSTUP_UPDATE_ROOT=https://mirrors.ustc.edu.cn/rust-static/rustup" >> /root/.bashrc && \
    echo "export RUSTUP_DIST_SERVER=https://mirrors.tuna.tsinghua.edu.cn/rustup" >> /root/.bashrc

RUN source /root/.bashrc

# 3. 安装 Rust
#    这里执行 curl 的脚本，并自动 yes (-y)
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

# 4. 将 Rust 安装目录加入 PATH
ENV PATH="/root/.cargo/bin:${PATH}"

# 5. 创建工作目录 (例如 /app)
WORKDIR /app

# 6. 复制 dedup/bff 目录到镜像的 /app/dedup/bff
#    假设你的 Dockerfile 与 dedup/ 同级或者上层，可以调整上下文以及 COPY 路径
COPY . /app/dedup/bff

# 7. 声明接下来要进入的工作目录 (可选，看你项目需求)
WORKDIR /app/dedup/bff

# 8. 你可以在此处选择是否编译依赖
#    如需先构建依赖,可以做 cargo build
RUN cargo build --release

ENTRYPOINT ["cargo", "run", "--release", "bff"]
# 设置默认启动命令，例如直接执行 cargo run
CMD ["--help"]