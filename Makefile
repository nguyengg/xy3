build:
	go build -o ./bin/ ./cmd/xy3
	GOOS=windows go build -o ./bin/ ./cmd/xy3
