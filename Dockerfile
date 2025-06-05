FROM python:3.10.12-slim-buster

WORKDIR /app

# 安装所需包

RUN echo "deb http://mirrors.aliyun.com/debian buster main contrib non-free" > /etc/apt/sources.list && \
    echo "deb http://mirrors.aliyun.com/debian-security buster/updates main" >> /etc/apt/sources.list && \
    echo "deb http://mirrors.aliyun.com/debian buster-updates main contrib non-free" >> /etc/apt/sources.list && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
        cmake \
        build-essential \
        wget \
        g++ \
        git \
        python3-distutils \
        python3-dev \
        libatlas-base-dev \
        aria2 && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* ~/.cache/pip && \
    wget http://gosspublic.alicdn.com/ossutil/1.7.13/ossutil64 -O /usr/local/bin/ossutil && \
    chmod +x /usr/local/bin/ossutil


COPY . /app/dclm-sci

# 进入项目目录
WORKDIR /app/dclm-sci

# 设置 Python 包源为阿里云，安装依赖 & 安装本地包
RUN pip config set global.index-url https://mirrors.aliyun.com/pypi/simple/ \
 && pip install --default-timeout=6000 -r requirements.txt --no-cache-dir \
 && python setup.py install

RUN pip install --default-timeout=6000 --use-deprecated=legacy-resolver --no-cache-dir -r requirements_fineweb.txt

# 给予脚本可执行权限
RUN chmod +x /app/dclm-sci/start.sh

# 用 entrypoint.sh 作为入口，可让用户在 docker run 时追加命令行参数
ENTRYPOINT ["/app/dclm-sci/start.sh"]
# CMD 可以留空，这样参数完全由 docker run 时候提供
CMD []