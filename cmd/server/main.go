package main

import (
	"flag"
	"log"
	"net/http"

	"github.com/chris64233/go-api-quota/internal/httpapi"
	"github.com/chris64233/go-api-quota/internal/quota"
)

func main() {
	addr := flag.String("addr", ":8080", "HTTP listen address")
	data := flag.String("data", "quota-store.json", "path to the quota data file")
	refundTTL := flag.Duration("refund-ttl", 0, "how long a consume can be refunded (0 = 24h)")
	flag.Parse()

	store, err := quota.OpenFileStore(*data)
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	var opts []quota.Option
	if *refundTTL > 0 {
		opts = append(opts, quota.WithRefundTTL(*refundTTL))
	}
	svc := quota.NewService(store, opts...)
	srv := httpapi.NewServer(svc)

	log.Printf("quota service listening on %s, data file %s", *addr, *data)
	log.Fatal(http.ListenAndServe(*addr, srv))
}
