// Copyright 2021 ChainSafe Systems
// SPDX-License-Identifier: LGPL-3.0-only

// Package mongo represent store driver for mongodb
package mongo

import (
	"context"
	"errors"
	"eth2-crawler/store/peerstore"
	"fmt"
	"time"

	"eth2-crawler/models"

	"eth2-crawler/utils/config"

	"github.com/libp2p/go-libp2p-core/peer"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

type mongoStore struct {
	client  *mongo.Client
	coll    *mongo.Collection
	timeout time.Duration
}

func (s *mongoStore) Upsert(ctx context.Context, peer *models.Peer) error {
	_, err := s.View(ctx, peer.ID)
	if err != nil {
		if errors.Is(err, peerstore.ErrPeerNotFound) {
			return s.Create(ctx, peer)
		}
		return err
	}

	return s.Update(ctx, peer)
}

func (s *mongoStore) Create(ctx context.Context, peer *models.Peer) error {
	_, err := s.View(ctx, peer.ID)
	if err != nil {
		if errors.Is(err, peerstore.ErrPeerNotFound) {
			_, err = s.coll.InsertOne(ctx, peer)
			return err
		}
		return err
	}
	return nil
}

func (s *mongoStore) Update(ctx context.Context, peer *models.Peer) error {
	filter := bson.D{
		{Key: "_id", Value: peer.ID},
	}
	_, err := s.coll.UpdateOne(ctx, filter, bson.D{{Key: "$set", Value: peer}})
	if err != nil {
		return err
	}
	return nil
}

func (s *mongoStore) Delete(ctx context.Context, peer *models.Peer) error {
	filter := bson.D{
		{Key: "_id", Value: peer.ID},
	}
	_, err := s.coll.DeleteOne(ctx, filter)
	if err != nil {
		return err
	}
	return nil
}

func (s *mongoStore) View(ctx context.Context, peerID peer.ID) (*models.Peer, error) {
	filter := bson.D{
		{Key: "_id", Value: peerID},
	}
	res := new(models.Peer)
	err := s.coll.FindOne(ctx, filter).Decode(res)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, peerstore.ErrPeerNotFound
		}
		return nil, err
	}

	return res, nil
}

// Todo: accept filter and find options to get limited information
func (s *mongoStore) ViewAll(ctx context.Context) ([]*models.Peer, error) {
	var peers []*models.Peer
	cursor, err := s.coll.Find(ctx, bson.D{{Key: "is_connectable", Value: true}})
	if err != nil {
		return nil, err
	}

	for cursor.Next(ctx) {
		// create a value into which the single document can be decoded
		peer := new(models.Peer)
		err := cursor.Decode(peer)
		if err != nil {
			return nil, err
		}

		peers = append(peers, peer)
	}
	return peers, nil
}

func (s *mongoStore) ListForJob(ctx context.Context, lastUpdated time.Duration, limit int) ([]*models.Peer, error) {
	var peers []*models.Peer
	timeToSkip := time.Now().Add(-lastUpdated).Unix()
	opts := options.Find()
	opts.SetLimit(int64(limit))
	opts.SetSort(bson.D{{Key: "last_updated", Value: 1}})
	filter := bson.D{{Key: "last_updated", Value: bson.D{{Key: "$lt", Value: timeToSkip}}}}
	cursor, err := s.coll.Find(ctx, filter, opts)
	if err != nil {
		return nil, err
	}

	for cursor.Next(ctx) {
		// create a value into which the single document can be decoded
		peer := new(models.Peer)
		err := cursor.Decode(peer)
		if err != nil {
			return nil, err
		}

		peers = append(peers, peer)
	}
	return peers, nil
}

type aggregateData struct {
	ID    string `json:"_id" bson:"_id"`
	Count int    `json:"count" bson:"count"`
}

func (s *mongoStore) AggregateByAgentName(ctx context.Context) ([]*models.AggregateData, error) {
	query := mongo.Pipeline{
		bson.D{
			{Key: "$group", Value: bson.D{
				{Key: "_id", Value: "$useragent.name"},
				{Key: "count", Value: bson.D{{Key: "$sum", Value: 1}}},
			}},
		},
		bson.D{
			{Key: "$match", Value: bson.D{
				{Key: "is_connectable", Value: true}}}},
	}

	cursor, err := s.coll.Aggregate(ctx, query)
	if err != nil {
		return nil, err
	}

	result := []*models.AggregateData{}
	for cursor.Next(ctx) {
		// create a value into which the single document can be decoded
		data := new(aggregateData)
		err := cursor.Decode(data)
		if err != nil {
			return nil, err
		}

		result = append(result, &models.AggregateData{Name: data.ID, Count: data.Count})
	}
	return result, nil
}

type clientVersionAggregation struct {
	ID       string                  `json:"_id" bson:"_id"`
	Count    int                     `json:"count" bson:"count"`
	Versions []*models.AggregateData `json:"versions" bson:"versions"`
}

func (s *mongoStore) AggregateByClientVersion(ctx context.Context) ([]*models.ClientVersionAggregation, error) {
	query := mongo.Pipeline{
		bson.D{
			{Key: "$group", Value: bson.D{
				{Key: "_id", Value: bson.D{
					{Key: "client", Value: "$useragent.name"},
					{Key: "version", Value: "$useragent.version"},
				}},
				{Key: "versionCount", Value: bson.D{{Key: "$sum", Value: 1}}},
			}},
		},
		bson.D{
			{Key: "$match", Value: bson.D{
				{Key: "is_connectable", Value: true}}}},
		bson.D{
			{Key: "$group", Value: bson.D{
				{Key: "_id", Value: "$_id.client"},
				{Key: "versions", Value: bson.D{
					{Key: "$push", Value: bson.D{
						{Key: "name", Value: "$_id.version"},
						{Key: "count", Value: "$versionCount"},
					}},
				}},
				{Key: "count", Value: bson.D{
					{Key: "$sum", Value: "$versionCount"},
				}},
			}},
		},
	}

	cursor, err := s.coll.Aggregate(ctx, query)
	if err != nil {
		return nil, err
	}

	result := []*models.ClientVersionAggregation{}
	for cursor.Next(ctx) {
		// create a value into which the single document can be decoded
		data := new(clientVersionAggregation)
		err := cursor.Decode(data)
		if err != nil {
			return nil, err
		}

		result = append(result, &models.ClientVersionAggregation{
			Client:   data.ID,
			Count:    data.Count,
			Versions: data.Versions,
		})
	}
	return result, nil
}

func (s *mongoStore) AggregateByOperatingSystem(ctx context.Context) ([]*models.AggregateData, error) {
	query := mongo.Pipeline{
		bson.D{
			{Key: "$group", Value: bson.D{
				{Key: "_id", Value: "$useragent.os"},
				{Key: "count", Value: bson.D{{Key: "$sum", Value: 1}}},
			}},
		},
		bson.D{
			{Key: "$match", Value: bson.D{
				{Key: "is_connectable", Value: true}}}},
	}
	cursor, err := s.coll.Aggregate(ctx, query)
	if err != nil {
		return nil, err
	}

	result := []*models.AggregateData{}
	for cursor.Next(ctx) {
		// create a value into which the single document can be decoded
		data := new(aggregateData)
		err := cursor.Decode(data)
		if err != nil {
			return nil, err
		}

		result = append(result, &models.AggregateData{Name: data.ID, Count: data.Count})
	}
	return result, nil
}

func (s *mongoStore) AggregateByCountry(ctx context.Context) ([]*models.AggregateData, error) {
	query := mongo.Pipeline{
		bson.D{
			{Key: "$group", Value: bson.D{
				{Key: "_id", Value: "$geolocation.country"},
				{Key: "count", Value: bson.D{{Key: "$sum", Value: 1}}},
			}},
		},
		bson.D{
			{Key: "$match", Value: bson.D{
				{Key: "is_connectable", Value: true}}}},
	}
	cursor, err := s.coll.Aggregate(ctx, query)
	if err != nil {
		return nil, err
	}

	result := []*models.AggregateData{}
	for cursor.Next(ctx) {
		// create a value into which the single document can be decoded
		data := new(aggregateData)
		err := cursor.Decode(data)
		if err != nil {
			return nil, err
		}

		result = append(result, &models.AggregateData{Name: data.ID, Count: data.Count})
	}
	return result, nil
}

func (s *mongoStore) AggregateByNetworkType(ctx context.Context) ([]*models.AggregateData, error) {
	query := mongo.Pipeline{
		bson.D{
			// avoid aggregation of entries without geolocation information
			{Key: "$match", Value: bson.D{
				{Key: "geolocation", Value: bson.D{{Key: "$ne", Value: nil}}},
			}},
		},
		bson.D{
			{Key: "$group", Value: bson.D{
				{Key: "_id", Value: "$geolocation.asn.type"},
				{Key: "count", Value: bson.D{{Key: "$sum", Value: 1}}},
			}},
		},
	}
	cursor, err := s.coll.Aggregate(ctx, query)
	if err != nil {
		return nil, err
	}

	result := []*models.AggregateData{}
	for cursor.Next(ctx) {
		// create a value into which the single document can be decoded
		data := new(aggregateData)
		err := cursor.Decode(data)
		if err != nil {
			return nil, err
		}

		result = append(result, &models.AggregateData{Name: data.ID, Count: data.Count})
	}
	return result, nil
}

// New creates new instance of Entry Store based on MongoDB
func New(cfg *config.Database) (peerstore.Provider, error) {
	timeout := time.Duration(cfg.Timeout) * time.Second
	opts := options.Client()

	opts.ApplyURI(cfg.URI)
	client, err := mongo.NewClient(opts)
	if err != nil {
		return nil, fmt.Errorf("connecton error [%s]: %w", opts.GetURI(), err)
	}

	// connect to the mongoDB cluster
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	err = client.Connect(ctx)
	if err != nil {
		return nil, err
	}

	// test the connection
	err = client.Ping(ctx, readpref.Primary())
	if err != nil {
		return nil, err
	}

	return &mongoStore{
		client:  client,
		coll:    client.Database(cfg.Database).Collection(cfg.Collection),
		timeout: timeout,
	}, nil
}
