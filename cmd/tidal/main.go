package main

import (
	"context"
	"fmt"
	"github.com/linlea666/tidal-crypto-intelligence/internal/tidal"
	"golang.org/x/crypto/bcrypt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"strings"
	"sync"
	"syscall"
	"time"
)

var version = "dev"

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
func main() {
	if len(os.Args) > 1 && os.Args[1] == "hash-password" {
		b := os.Getenv("TIDAL_PASSWORD")
		if len(b) < 12 {
			fmt.Fprintln(os.Stderr, "Set TIDAL_PASSWORD to at least 12 characters")
			os.Exit(1)
		}
		h, err := bcrypt.GenerateFromPassword([]byte(b), bcrypt.DefaultCost)
		if err != nil {
			panic(err)
		}
		fmt.Println(string(h))
		return
	}
	hash := os.Getenv("TIDAL_PASSWORD_HASH")
	if name := os.Getenv("TIDAL_PASSWORD_HASH_FILE"); name != "" {
		b, err := os.ReadFile(name)
		if err != nil {
			panic(err)
		}
		hash = strings.TrimSpace(string(b))
	}
	if _, err := bcrypt.Cost([]byte(hash)); err != nil {
		slog.Error("TIDAL_PASSWORD_HASH must be a valid bcrypt hash")
		os.Exit(1)
	}
	debug.SetMemoryLimit(700 << 20)
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	store, err := tidal.OpenStore(env("TIDAL_DATA", "data"))
	if err != nil {
		panic(err)
	}
	defer store.Close()
	engine := tidal.NewEngine()
	collector := tidal.NewCollector(engine)
	whales := tidal.NewWhales(collector)
	if err = store.Initialize(engine, whales); err != nil {
		panic(err)
	}
	collector.Start(ctx)
	whales.Start(ctx)
	go func() {
		tick := time.NewTicker(time.Second)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case t := <-tick.C:
				engine.Tick(t.UTC())
			}
		}
	}()
	var workers sync.WaitGroup
	workers.Add(1)
	go func() { defer workers.Done(); store.Run(ctx, engine, whales) }()
	api := tidal.NewServer(engine, store, whales, hash, env("TIDAL_WEB", "web/dist/client"), version, env("TIDAL_COOKIE_SECURE", "true") == "true")
	server := &http.Server{Addr: env("TIDAL_ADDR", "127.0.0.1:8080"), Handler: api.Handler(), ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 75 * time.Second, MaxHeaderBytes: 32 << 10}
	go func() {
		<-ctx.Done()
		shutdown, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		server.Shutdown(shutdown)
	}()
	slog.Info("Tidal started", "address", server.Addr, "version", version)
	if err = server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		slog.Error("server stopped", "error", err)
		os.Exit(1)
	}
	cancel()
	workers.Wait()
}
