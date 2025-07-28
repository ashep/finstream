package main

import (
	"os"

	"github.com/ashep/finstream/internal/app"
	"github.com/ashep/go-app/runner"
)

func main() {
	r := runner.New(app.New).
		WithConsoleLogWriter().
		WithDefaultHTTPServer().
		WithDefaultMetricsHandler()

	if os.Getenv("APP_LOGSERVER_URL") != "" {
		r = r.WithDefaultHTTPLogWriter()
	}

	r.Run()
}
