FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags='-s -w' -o /out/liveresize ./cmd/liveresize

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/liveresize /liveresize
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/liveresize"]
