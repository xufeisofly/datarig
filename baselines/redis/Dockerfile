# 基础镜像使用 Debian，兼容性好
FROM debian:bullseye-slim

# 设置环境变量避免交互提示
ENV DEBIAN_FRONTEND=noninteractive

# 安装 redis-cli 和常用网络工具
RUN apt update && \
    apt install -y redis-tools iputils-ping dnsutils telnet netcat && \
    apt clean && rm -rf /var/lib/apt/lists/*

# 启动容器后进入 shell，方便手动测试
CMD ["bash"]