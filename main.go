package main

import (
	"github.com/ashep/finstream/internal/app"
	"github.com/ashep/go-app/runner"
)

func main() {
	r := runner.New(app.New).
		WithConsoleLogWriter().
		WithDefaultHTTPLogWriter().
		WithDefaultHTTPServer().
		WithDefaultHealthHandler().
		WithDefaultMetricsHandler()

	r.Run()
}
