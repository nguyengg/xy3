build:
	GOEXPERIMENT=greenteagc go build -o ./bin/ ./cmd/xy3
	GOEXPERIMENT=greenteagc GOOS=windows go build -o ./bin/ ./cmd/xy3
