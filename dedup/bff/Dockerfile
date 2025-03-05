# 使用 Ubuntu 22.04.5 LTS 作为基础镜像
FROM FROM 10.200.88.53/public/ubuntu@sha256:b729117166cef7a731b8b5d2aa7ab0aa12b7b6b68b4a016199b85d1f7de68da9

# 切换时区交互提示，避免一些安装中的时区询问
ENV DEBIAN_FRONTEND=noninteractive
ENV RUSTUP_UPDATE_ROOT="https://mirrors.ustc.edu.cn/rust-static/rustup"
ENV RUSTUP_DIST_SERVER="https://mirrors.tuna.tsinghua.edu.cn/rustup"

RUN apt-get update && \
    apt-get install -y libssl-dev && \
    rm -rf /var/lib/apt/lists/*

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /app

COPY . /app/dedup/bff

WORKDIR /app/dedup/bff

RUN cargo build --release

ENTRYPOINT ["cargo", "run", "--release", "bff"]

CMD ["--help"]