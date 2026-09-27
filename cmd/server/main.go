package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/chris64233/go-api-quota/internal/httpapi"
	"github.com/chris64233/go-api-quota/internal/quota"
)

func main() {
	addr := flag.String("addr", ":8080", "HTTP listen address")
	data := flag.String("data", "quota-store.json", "path to the quota data file")
	refundTTL := flag.Duration("refund-ttl", 0, "how long a consume can be refunded (0 = 24h)")
	reservationTTL := flag.Duration("reservation-ttl", 0, "how long a reservation stays valid (0 = 5m)")
	expiryInterval := flag.Duration("expiry-interval", time.Second, "interval between reservation expiry sweeps")
	flag.Parse()

	store, err := quota.OpenFileStore(*data)
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	var opts []quota.Option
	if *refundTTL > 0 {
		opts = append(opts, quota.WithRefundTTL(*refundTTL))
	}
	if *reservationTTL > 0 {
		opts = append(opts, quota.WithReservationTTL(*reservationTTL))
	}
	svc := quota.NewService(store, opts...)

	// 启动时先回收一次重启期间到期的预占，之后按固定间隔扫描。
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if _, err := svc.ExpireDue(ctx); err != nil {
		log.Printf("initial expiry sweep: %v", err)
	}
	go svc.RunExpiryLoop(ctx, *expiryInterval)

	srv := httpapi.NewServer(svc)
	httpServer := &http.Server{Addr: *addr, Handler: srv}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(shutdownCtx)
	}()

	log.Printf("quota service listening on %s, data file %s", *addr, *data)
	if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
