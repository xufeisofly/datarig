# ----------- 构建阶段：安装依赖和打包项目 -------------
FROM python:3.10.12-slim-buster AS builder

WORKDIR /app

# 安装构建所需工具
RUN apt update && apt install -y \
    cmake \
    build-essential \
    g++ \
    git \
    aria2 \
 && apt clean && rm -rf /var/lib/apt/lists/*

# 设置国内镜像源，加速 pip
RUN pip config set global.index-url https://mirrors.aliyun.com/pypi/simple/

# 拷贝项目代码
COPY . /app/dclm-sci

WORKDIR /app/dclm-sci

# 安装依赖到临时目录（使用 --target 方便复制）
RUN pip install --default-timeout=6000 --upgrade pip \
 && pip install --target=/install -r requirements.txt \
 && python setup.py install --prefix=/install

# ----------- 运行阶段：极简环境 -------------
FROM python:3.10.12-slim-buster

WORKDIR /app/dclm-sci

# 复制安装后的文件到目标镜像
COPY --from=builder /install /usr/local
COPY --from=builder /app/dclm-sci /app/dclm-sci

# 给予脚本可执行权限
RUN chmod +x /app/dclm-sci/start.sh

# 启动命令
ENTRYPOINT ["/app/dclm-sci/start.sh"]
CMD []