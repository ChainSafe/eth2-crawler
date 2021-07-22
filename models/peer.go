// Copyright 2021 ChainSafe Systems
// SPDX-License-Identifier: LGPL-3.0-only

// Package models represent the models for the service
package models

import (
	"encoding/hex"
	"encoding/json"
	"strings"
	"time"

	"eth2-crawler/crawler/util"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p/enode"
	ic "github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/protolambda/zrnt/eth2/beacon/common"
)

// UserAgent holds peer's client related info
type UserAgent struct {
	Name    string `json:"name" bson:"name"`
	Version string `json:"version" bson:"version"`
	OS      string `json:"os" bson:"os"`
}

// GeoLocation holds peer's geo location related info
type GeoLocation struct {
	ISP          string `json:"isp" bson:"isp"`
	Organization string `json:"organization" bson:"organization"`
	Country      string `json:"country_name" bson:"country"`
	State        string `json:"state" bson:"state"`
	City         string `json:"city" bson:"city"`
	Latitude     string `json:"latitude" bson:"latitude"`
	Longitude    string `json:"longitude" bson:"longitude"`
}

// Peer holds all information of a eth2 peer
type Peer struct {
	ID     peer.ID `json:"id" bson:"_id"`
	NodeID string  `json:"node_id" bson:"node_id"`
	Pubkey string  `json:"pubkey"`

	IP      string   `json:"ip"`
	TCPPort int      `json:"tcp_port"`
	UDPPort int      `json:"udp_port"`
	Addrs   []string `json:"addrs,omitempty"`

	Attnets  common.AttnetBits `json:"enr_attnets,omitempty"`
	Eth2Data *common.Eth2Data  `json:"eth2_data" bson:"-"`

	ProtocolVersion string       `json:"protocol_version,omitempty"`
	UserAgent       *UserAgent   `json:"user_agent,omitempty"`
	GeoLocation     *GeoLocation `json:"geolocation" bson:"geolocation"`

	IsConnectable bool  `json:"is_connectable"`
	LastConnected int64 `json:"last_connected"`
}

// NewPeer initializes new peer
func NewPeer(node *enode.Node, eth2Data *common.Eth2Data) (*Peer, error) {
	pk := ic.PubKey((*ic.Secp256k1PublicKey)(node.Pubkey()))
	pkByte, err := pk.Raw()
	if err != nil {
		return nil, err
	}
	addr, err := util.AddrsFromEnode(node)
	if err != nil {
		return nil, err
	}
	addrStr := make([]string, 0)
	for _, madd := range addr.Addrs {
		addrStr = append(addrStr, madd.String())
	}

	attnetsVal := common.AttnetBits{}
	attnets, err := util.ParseEnrAttnets(node)
	if err == nil {
		attnetsVal = *attnets
	}
	return &Peer{
		ID:       addr.ID,
		NodeID:   node.ID().String(),
		Pubkey:   hex.EncodeToString(pkByte),
		IP:       node.IP().String(),
		TCPPort:  node.TCP(),
		UDPPort:  node.UDP(),
		Addrs:    addrStr,
		Eth2Data: eth2Data,
		Attnets:  attnetsVal,
	}, nil
}

// SetProtocolVersion sets peer's protocol version
func (p *Peer) SetProtocolVersion(pv string) {
	p.ProtocolVersion = pv
}

// SetAgentVersion sets peer's agent version
func (p *Peer) SetAgentVersion(ag string) {
	// split the ag based on this format. might not be identical with each type of node
	// ag = Name/Version/OS(or git commit hash for Prysm)
	userAgent := new(UserAgent)
	parts := strings.Split(ag, "/")
	userAgent.Name = parts[0]
	if len(parts) > 1 {
		userAgent.Version = parts[1]
	}
	if len(parts) > 2 {
		userAgent.OS = parts[2]
	}
	p.UserAgent = userAgent
}

// SetConnectionStatus sets connection status and date
func (p *Peer) SetConnectionStatus(status bool) {
	p.IsConnectable = status
	if status {
		p.LastConnected = time.Now().Unix()
	}
}

// SetGeoLocation sets the geolocation information
func (p *Peer) SetGeoLocation(geoLocation *GeoLocation) {
	p.GeoLocation = geoLocation
}

// GetPeerInfo returns peer's AddrInfo
func (p *Peer) GetPeerInfo() *peer.AddrInfo {
	maddrs := make([]ma.Multiaddr, 0)
	for _, v := range p.Addrs {
		madd, _ := ma.NewMultiaddr(v)
		maddrs = append(maddrs, madd)
	}
	return &peer.AddrInfo{
		ID:    p.ID,
		Addrs: maddrs,
	}
}

// String returns peer object's json form in string
func (p *Peer) String() string {
	if p == nil {
		return "no data available"
	} else {
		dat, err := json.Marshal(p)
		if err != nil {
			return "failed to format peer data"
		}
		return string(dat)
	}
}

// Log returns log ctx from peer
func (p *Peer) Log() log.Ctx {
	dat, err := json.Marshal(p)
	if err != nil {
		return log.Ctx{}
	}
	val := log.Ctx{}
	err = json.Unmarshal(dat, &val)
	if err != nil {
		return log.Ctx{}
	}
	return val
}
