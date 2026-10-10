package main

import (
	"context"
	_ "embed"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

//go:embed static/index.html
var indexHTML []byte

func main() {
	c, err := loadConfig(os.Args[1:], os.Stderr)
	if errors.Is(err, flag.ErrHelp) {
		return
	}
	if err != nil {
		slog.Error("configuration failed", "error", err)
		os.Exit(1)
	}
	server := &http.Server{Addr: c.Listen, Handler: newHandler(c), ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
	}()
	slog.Info("Redis Explorer is ready", "address", c.Listen)
	if err = server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
