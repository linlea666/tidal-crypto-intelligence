package main

import (
	"context"
	"fmt"
	"github.com/linlea666/tidal-crypto-intelligence/internal/datahub"
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
	if len(os.Args) == 4 && os.Args[1] == "backup-db" {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		if err := datahub.BackupFile(ctx, os.Args[2], os.Args[3]); err != nil {
			fmt.Fprintln(os.Stderr, "SQLite online backup failed:", err)
			os.Exit(1)
		}
		return
	}
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
	debug.SetMemoryLimit(512 << 20)
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	store, err := tidal.OpenStore(env("TIDAL_DATA", "data"))
	if err != nil {
		panic(err)
	}
	defer store.Close()
	engine := tidal.NewEngine()
	// Legacy collectors remain only as tested migration/rollback code. No direct
	// orderbook, derivatives or Hyperliquid discovery goroutines start in V2.
	whales := tidal.NewWhales(tidal.NewCollector(engine))
	key := os.Getenv("COINGLASS_API_KEY")
	if name := os.Getenv("COINGLASS_API_KEY_FILE"); name != "" {
		b, e := os.ReadFile(name)
		if e != nil {
			slog.Error("CoinGlass key file unavailable")
			os.Exit(1)
		}
		key = strings.TrimSpace(string(b))
	}
	hub, err := datahub.Open(datahub.Config{Root: env("TIDAL_DATA", "data") + "/v2", BaseURL: env("COINGLASS_BASE_URL", "https://proxy.keystore.com.cn/api/v1/proxy/coinglass"), Key: key, Offline: os.Getenv("TIDAL_OFFLINE") == "true"})
	if err != nil {
		panic(err)
	}
	defer hub.Store.Close()
	var workers sync.WaitGroup
	workers.Add(1)
	go func() { defer workers.Done(); hub.Run(ctx) }()
	workers.Add(1)
	go func() {
		defer workers.Done()
		tick := time.NewTicker(time.Minute)
		defer tick.Stop()
		for {
			set := store.Settings()
			hub.Store.SetLimits(set.RetentionDays, set.BudgetGB, set.MinFreeGB)
			if e := store.MaintainArchive(time.Now().UTC()); e != nil {
				slog.Warn("legacy archive maintenance failed", "error", e)
			}
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
			}
		}
	}()
	api := tidal.NewServer(engine, store, whales, hash, env("TIDAL_WEB", "web/dist/client"), version, env("TIDAL_COOKIE_SECURE", "true") == "true")
	api.Hub = hub
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
