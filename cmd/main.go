// Copyright 2021 ChainSafe Systems
// SPDX-License-Identifier: LGPL-3.0-only

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"time"

	"eth2-crawler/crawler"
	"eth2-crawler/graph"
	"eth2-crawler/graph/generated"
	"eth2-crawler/resolver/ipdata"
	peerStore "eth2-crawler/store/peerstore/mongo"
	recordStore "eth2-crawler/store/record/mongo"
	"eth2-crawler/utils/config"
	"eth2-crawler/utils/server"

	"github.com/99designs/gqlgen/graphql/handler"
	"github.com/99designs/gqlgen/graphql/playground"
)

func main() {
	cfgPath := flag.String("p", "./cmd/config/config.dev.yaml", "The configuration path")
	flag.Parse()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatalf("error loading configuration: %s", err.Error())
	}

	peerStore, err := peerStore.New(cfg.Database)
	if err != nil {
		log.Fatalf("error Initializing the peer store: %s", err.Error())
	}

	historyStore, err := recordStore.New(cfg.Database)
	if err != nil {
		log.Fatalf("error Initializing the record store: %s", err.Error())
	}

	resolverService, err := ipdata.New(cfg.Resolver.APIKey, time.Duration(cfg.Resolver.Timeout)*time.Second)
	if err != nil {
		log.Fatalf("error Initializing the ip resolver: %s", err.Error())
	}

	// TODO collect config from a config files or from command args and pass to Start()
	go crawler.Start(peerStore, historyStore, resolverService)

	srv := handler.NewDefaultServer(generated.NewExecutableSchema(generated.Config{Resolvers: graph.NewResolver(peerStore, historyStore)}))

	router := http.NewServeMux()
	// TODO: make playground accessible only in Dev mode
	router.Handle("/", playground.Handler("GraphQL playground", "/query"))
	router.Handle("/query", srv)
	// TODO: setup proper status handler
	router.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "{ \"status\": \"up\" }")
	})

	server.Start(context.TODO(), cfg.Server, router)
}
