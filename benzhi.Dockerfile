# 官方 Go 镜像，自带完整工具链
FROM golang:1.22

WORKDIR /app

# 复制所有项目文件
COPY . .

# 预编译一次，把编译缓存留在镜像里（不影响源码，模型仍可自由修改）
RUN go build ./...

CMD ["bash"]
