# multi-stage build

# stage 1: compile go files
FROM golang:1.21-alpine AS builder

WORKDIR /app
COPY dispatcher.go .
COPY static ./static
RUN go build -o dispatcher dispatcher.go


# stage 2: copy all built files from previous stage (1) to current stage (2)
FROM alpine:3.18
WORKDIR /app
# --from=<stage-name> <source> <destination>
COPY --from=builder /app/dispatcher .
COPY --from=builder /app/static ./static
# Instead of writing 2 commands, it could be replaced by one-line command
# COPY --from=builder /app/dispatcher /app/static ./
EXPOSE 80

CMD ["./dispatcher"]