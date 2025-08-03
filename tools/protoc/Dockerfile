ARG GO_VERSION=1.22.2
ARG PROTOC_VERSION=31.1

FROM golang:${GO_VERSION}-alpine AS build
ARG PROTOC_VERSION

RUN apk add --no-cache bash curl unzip git tzdata \
 && cp /usr/share/zoneinfo/Asia/Bishkek /etc/localtime \
 && echo "Asia/Bishkek" > /etc/timezone

RUN curl -L "https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-linux-x86_64.zip" \
      -o /tmp/protoc.zip \
 && unzip -q /tmp/protoc.zip -d /usr/local \
 && rm /tmp/protoc.zip

RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.6 \
 && go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.4.0

RUN git clone --depth 1 https://github.com/googleapis/googleapis.git /googleapis

WORKDIR /workspace
CMD ["bash"]
