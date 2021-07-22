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
	ipResolver "eth2-crawler/resolver"
	mongoStore "eth2-crawler/store/mongo"
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

	peerStore, err := mongoStore.New(cfg.Database)
	if err != nil {
		log.Fatalf("error Initializing the peer store: %s", err.Error())
	}

	resolverService := ipResolver.New(cfg.Resolver.APIKey, time.Duration(cfg.Resolver.Timeout)*time.Second)

	// TODO collect config from a config files or from command args and pass to Start()
	go crawler.Start(peerStore, resolverService)

	srv := handler.NewDefaultServer(generated.NewExecutableSchema(generated.Config{Resolvers: graph.NewResolver(peerStore)}))

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
