package main

import (
	"flag"
	"log"
	"net/http"

	apiquota "github.com/chris64233/go-api-quota"
)

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	db := flag.String("db", "quota-store.json", "path to the persistent store file")
	flag.Parse()

	store, err := apiquota.OpenStore(*db)
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	svc := apiquota.NewService(store)
	log.Printf("quota service listening on %s, store=%s", *addr, *db)
	log.Fatal(http.ListenAndServe(*addr, apiquota.NewHandler(svc)))
}
