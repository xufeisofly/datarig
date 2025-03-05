# 使用 Ubuntu 22.04.5 LTS 作为基础镜像
FROM ubuntu:22.04

# 切换时区交互提示，避免一些安装中的时区询问
ENV DEBIAN_FRONTEND=noninteractive
ENV RUSTUP_UPDATE_ROOT=https://mirrors.ustc.edu.cn/rust-static/rustup
ENV RUSTUP_DIST_SERVER=https://mirrors.ustc.edu.cn/rust-static/rustup

RUN apt-get update && \
    apt-get install -y curl sudo libssl-dev && \
    rm -rf /var/lib/apt/lists/*

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y && \
    . /root/.cargo/env && \
    cargo --version  # 测试 cargo 是否可用

ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /app

COPY . /app/dedup/bff

WORKDIR /app/dedup/bff

RUN . /root/.cargo/env && cargo build --release

ENTRYPOINT ["cargo", "run", "--release", "bff"]

CMD ["--help"]