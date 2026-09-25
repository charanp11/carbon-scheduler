# Build stage: compiles a static binary, nothing else ends up in the
# final image.
FROM golang:1.26-alpine AS build
RUN apk add --no-cache ca-certificates
WORKDIR /src
COPY go.mod ./
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -o /out/scheduler ./cmd/scheduler

# Final stage: a minimal, non-root image. No shell, no package manager,
# nothing an attacker could use if they ever got code execution inside
# the container.
FROM scratch
COPY --from=build /out/scheduler /scheduler
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

# Runs as an unprivileged UID; scratch has no /etc/passwd to name it.
USER 65532:65532

EXPOSE 8080
ENTRYPOINT ["/scheduler"]
