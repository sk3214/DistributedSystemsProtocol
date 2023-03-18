FROM golang:alpine
WORKDIR /go
RUN mkdir 586
WORKDIR /go/586
COPY . .
RUN apk add --no-cache bash gcc musl-dev
CMD ["bash"]
