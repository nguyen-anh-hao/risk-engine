FROM golang:1.25 AS build
WORKDIR /src
COPY go.mod go.sum ./
COPY cmd ./cmd
COPY internal ./internal
RUN go build -o /bin/risk ./cmd

FROM debian:bookworm-slim
COPY --from=build /bin/risk /usr/local/bin/risk
EXPOSE 9002
ENTRYPOINT ["risk"]
