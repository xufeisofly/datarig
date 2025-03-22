# ----------------- 构建阶段 -----------------
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

# 设置 pip 国内镜像源，加速 pip 安装
RUN pip config set global.index-url https://mirrors.aliyun.com/pypi/simple/

# 拷贝项目代码到构建镜像中
COPY . /app/dclm-sci

WORKDIR /app/dclm-sci

# 删除多余的 .egg-info 目录，防止重复
RUN find . -maxdepth 1 -name "*.egg-info" -exec rm -rf {} +

# 升级 pip 并安装依赖到 /install 目录
RUN pip install --default-timeout=6000 --upgrade pip
RUN pip install --default-timeout=6000 --target=/install -r requirements.txt

# 安装本地包到 /install
RUN python setup.py install --prefix=/install

# ----------------- 运行阶段 -----------------
FROM python:3.10.12-slim-buster

WORKDIR /app/dclm-sci

# 复制构建阶段安装好的文件
COPY --from=builder /install /usr/local
# 复制项目代码（不需要包含构建工具）
COPY --from=builder /app/dclm-sci /app/dclm-sci

# 设置启动脚本可执行权限
RUN chmod +x /app/dclm-sci/start.sh

ENTRYPOINT ["/app/dclm-sci/start.sh"]
CMD []