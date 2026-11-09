FROM python:3.9-slim

# 设置工作目录
WORKDIR /app

# 安装 Debian 版本的 genisoimage 和时区工具
RUN apt-get update && apt-get install -y \
    genisoimage \
    tzdata \
    && rm -rf /var/lib/apt/lists/*

# 复制依赖并安装
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# 复制项目代码
COPY app.py /app/app.py
COPY templates /app/templates

# 创建必要的目录
RUN mkdir -p /data /output

# 暴露端口
EXPOSE 5000

# 启动命令
CMD ["python", "app.py"]