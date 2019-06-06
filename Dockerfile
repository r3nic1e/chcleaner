FROM golang:1.12

WORKDIR /chcleaner

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -v -a -installsuffix cgo -o chcleaner cmd/chcleaner/main.go

FROM scratch
COPY --from=0 /chcleaner/chcleaner /

ENTRYPOINT ["/chcleaner"]
