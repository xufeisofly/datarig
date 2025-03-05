FROM cr.registry.res.cloud.zhejianglab.com/datahub/python:3.10.12-slim-buster
WORKDIR /app

RUN apt update && apt install -y \
    cmake \
    build-essential \
    g++ \
    git \
    aria2 \
    && apt clean && rm -rf /var/lib/apt/lists/*

RUN git clone http://xiaohengjin%40zhejianglab.org:1d57e7c77dbd674ade5a35e1ec950a2e@gitee.zhejianglab.com:80/enterprise/dclm-sci.git
WORKDIR /app/dclm-sci
RUN pip config set global.index-url https://mirrors.aliyun.com/pypi/simple/
RUN pip install -r requirements.txt
RUN python setup.py install

RUN mkdir -p /root/dataprocess/dclm_pool/1b-1x
COPY CC_shard_00000000.jsonl.zst /root/dataprocess/dclm_pool/1b-1x/

RUN chmod 777 /app/dclm-sci/script/process/start.sh
# 添加一个总是变化的命令,使后续步骤不使用缓存, docker build --build-arg CACHEBUST=$(date +%s)
ARG CACHEBUST=1
RUN git pull
ENTRYPOINT ["/bin/bash", "-c", "/app/dclm-sci/script/process/start.sh"]