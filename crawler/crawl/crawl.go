// Copyright 2021 ChainSafe Systems
// SPDX-License-Identifier: LGPL-3.0-only

package crawl

import (
	"context"
	"crypto/ecdsa"
	"eth2-crawler/crawler/p2p"
	reqresp "eth2-crawler/crawler/rpc/request"
	"eth2-crawler/crawler/util"
	"eth2-crawler/models"
	ipResolver "eth2-crawler/resolver"
	"eth2-crawler/store/peerstore"
	"eth2-crawler/store/record"
	"time"

	"github.com/protolambda/zrnt/eth2/beacon/common"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p/enode"
)

// Eth2 ForkDigests for different Networks/Forks
var (
	MainnetPhase0ForkDigest = "0xb5303f2a"
	MainnetAltairForkDigest = "0xafcaaba0"
)

type crawler struct {
	disc            resolver
	peerStore       peerstore.Provider
	historyStore    record.Provider
	ipResolver      ipResolver.Provider
	iter            enode.Iterator
	nodeCh          chan *enode.Node
	privateKey      *ecdsa.PrivateKey
	host            p2p.Host
	jobs            chan *models.Peer
	jobsConcurrency int
}

// resolver holds methods of discovery v5
type resolver interface {
	Ping(n *enode.Node) error
}

// newCrawler inits new crawler service
func newCrawler(disc resolver, peerStore peerstore.Provider, historyStore record.Provider,
	ipResolver ipResolver.Provider, privateKey *ecdsa.PrivateKey, iter enode.Iterator,
	host p2p.Host, jobConcurrency int) *crawler {
	c := &crawler{
		disc:            disc,
		peerStore:       peerStore,
		historyStore:    historyStore,
		ipResolver:      ipResolver,
		privateKey:      privateKey,
		iter:            iter,
		nodeCh:          make(chan *enode.Node),
		host:            host,
		jobs:            make(chan *models.Peer, jobConcurrency),
		jobsConcurrency: jobConcurrency,
	}
	return c
}

// start runs the crawler
func (c *crawler) start(ctx context.Context) {
	doneCh := make(chan enode.Iterator)
	go c.runIterator(ctx, doneCh, c.iter)
	for {
		select {
		case n := <-c.nodeCh:
			c.storePeer(ctx, n)
		case <-doneCh:
			// crawling finished
			log.Info("finished iterator")
			return
		}
	}
}

// runIterator uses the node iterator and sends node data through channel
func (c *crawler) runIterator(ctx context.Context, doneCh chan enode.Iterator, it enode.Iterator) {
	defer func() { doneCh <- it }()
	for it.Next() {
		select {
		case c.nodeCh <- it.Node():
		case <-ctx.Done():
			return
		}
	}
}

func (c *crawler) storePeer(ctx context.Context, node *enode.Node) {
	// only consider the node having tcp port exported
	if node.TCP() == 0 {
		return
	}

	// filter only eth2 nodes
	eth2Data, err := util.ParseEnrEth2Data(node)
	if err != nil { // not eth2 nodes
		return
	}

	// Check whether the ForkDigest of the received peer matches the
	// network/fork that we want to crawl
	if eth2Data.ForkDigest.String() == MainnetAltairForkDigest {
		log.Debug("found a eth2 node", log.Ctx{"node": node})
		// get basic info
		peer, err := models.NewPeer(node, eth2Data)
		if err != nil {
			return
		}
		// save to db if not exists
		err = c.peerStore.Create(ctx, peer)
		if err != nil {
			log.Error("err inserting peer", log.Ctx{"err": err, "peer": peer.String()})
		}
	}
}

func (c *crawler) updatePeer(ctx context.Context) {
	c.runBGWorkersPool(ctx)
	for {
		select {
		case <-ctx.Done():
			log.Error("update peer job context was canceled", log.Ctx{"err": ctx.Err()})
		default:
			c.selectPendingAndExecute(ctx)
		}
		time.Sleep(5 * time.Second)
	}
}

func (c *crawler) selectPendingAndExecute(ctx context.Context) {
	// get peers that was updated 24 hours ago
	reqs, err := c.peerStore.ListForJob(ctx, time.Hour*24, c.jobsConcurrency)
	if err != nil {
		log.Error("error getting list from peerstore", log.Ctx{"err": err})
		return
	}
	for _, req := range reqs {
		select {
		case <-ctx.Done():
			log.Error("update selector stopped", log.Ctx{"err": ctx.Err()})
			return
		default:
			c.jobs <- req
		}
	}
}

func (c *crawler) runBGWorkersPool(ctx context.Context) {
	for i := 0; i < c.jobsConcurrency; i++ {
		go c.bgWorker(ctx)
	}
}

func (c *crawler) bgWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			log.Error("context canceled", log.Ctx{"err": ctx.Err()})
			return
		case req := <-c.jobs:
			c.updatePeerInfo(ctx, req)
		}
	}
}

func (c *crawler) updatePeerInfo(ctx context.Context, peer *models.Peer) {
	// update connection status, agent version, sync status
	isConnectable := c.collectNodeInfoRetryer(ctx, peer)
	if isConnectable {
		peer.SetConnectionStatus(true)
		peer.Score = models.ScoreGood
		peer.LastConnected = time.Now().Unix()
		// update geolocation
		if peer.GeoLocation == nil {
			c.updateGeolocation(ctx, peer)
		}
	} else {
		peer.Score--
	}
	// remove the node if it has bad score
	if peer.Score <= models.ScoreBad {
		log.Info("deleting node for bad score", log.Ctx{"peer_id": peer.ID})
		err := c.peerStore.Delete(ctx, peer)
		if err != nil {
			log.Error("failed on deleting from peerstore", log.Ctx{"err": err})
		}
		return
	}
	peer.LastUpdated = time.Now().Unix()
	err := c.peerStore.Update(ctx, peer)
	if err != nil {
		log.Error("failed on updating peerstore", log.Ctx{"err": err})
	}
}

func (c *crawler) collectNodeInfoRetryer(ctx context.Context, peer *models.Peer) bool {
	count := 0
	var err error
	var ag, pv string
	for count < 20 {
		time.Sleep(time.Second * 5)
		count++

		err = c.host.Connect(ctx, *peer.GetPeerInfo())
		if err != nil {
			continue
		}
		// get status
		var status *common.Status
		status, err = c.host.FetchStatus(c.host.NewStream, ctx, peer, new(reqresp.SnappyCompression))
		if err != nil || status == nil {
			continue
		}
		ag, err = c.host.GetAgentVersion(peer.ID)
		if err != nil {
			continue
		} else {
			peer.SetUserAgent(ag)
		}

		pv, err = c.host.GetProtocolVersion(peer.ID)
		if err != nil {
			continue
		} else {
			peer.SetProtocolVersion(pv)
		}
		// set sync status
		peer.SetSyncStatus(int64(status.HeadSlot))
		log.Info("successfully collected all info", peer.Log())
		return true
	}
	// unsuccessful
	log.Error("failed on retryer", log.Ctx{
		"attempt": count,
		"error":   err,
	})
	return false
}

func (c *crawler) updateGeolocation(ctx context.Context, peer *models.Peer) {
	geoLoc, err := c.ipResolver.GetGeoLocation(ctx, peer.IP)
	if err != nil {
		log.Error("unable to get geo information", log.Ctx{
			"error":   err,
			"ip_addr": peer.IP,
		})
		return
	}
	peer.SetGeoLocation(geoLoc)
}

func (c *crawler) insertToHistory() {
	ctx := context.Background()
	// get count
	aggregateData, err := c.peerStore.AggregateBySyncStatus(ctx)
	if err != nil {
		log.Error("error getting sync status", log.Ctx{"err": err})
	}

	history := models.NewHistory(aggregateData.Synced, aggregateData.Total)
	err = c.historyStore.Create(ctx, history)
	if err != nil {
		log.Error("error inserting sync status", log.Ctx{"err": err})
	}
}
