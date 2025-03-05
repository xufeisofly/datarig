# FROM cr.registry.res.cloud.zhejianglab.com/datahub/python:3.10.12-slim-buster
FROM python:3.10.12-slim-buster

WORKDIR /app

# 安装所需包
RUN apt update && apt install -y \
    cmake \
    build-essential \
    g++ \
    git \
    aria2 \
    && apt clean && rm -rf /var/lib/apt/lists/*

COPY . /app/dclm-sci

# 进入项目目录
WORKDIR /app/dclm-sci

# 设置 Python 包源为阿里云，安装依赖 & 安装本地包
RUN pip config set global.index-url https://mirrors.aliyun.com/pypi/simple/ \
 && python setup.py install

# 给予脚本可执行权限
RUN chmod +x /app/dclm-sci/start.sh

# 用 entrypoint.sh 作为入口，可让用户在 docker run 时追加命令行参数
ENTRYPOINT ["/app/dclm-sci/start.sh"]
# CMD 可以留空，这样参数完全由 docker run 时候提供
CMD []