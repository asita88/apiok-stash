package es

import (
	"context"
	"sync"
	"time"

	"github.com/kevwan/go-stash/stash/format"
	"github.com/olivere/elastic/v7"
	"github.com/zeromicro/go-zero/core/fx"
	"github.com/zeromicro/go-zero/core/lang"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/syncx"
)

type (
	Index struct {
		client       *elastic.Client
		indexFormat  func(map[string]interface{}) string
		indices      map[string]lang.PlaceholderType
		lock         sync.RWMutex
		singleFlight syncx.SingleFlight
	}
)

func NewIndex(client *elastic.Client, indexFormat string, loc *time.Location) *Index {
	return &Index{
		client:       client,
		indexFormat:  format.Format(indexFormat, loc),
		indices:      make(map[string]lang.PlaceholderType),
		singleFlight: syncx.NewSingleFlight(),
	}
}

func (idx *Index) GetIndex(m map[string]interface{}) string {
	index := idx.indexFormat(m)
	idx.lock.RLock()
	if _, ok := idx.indices[index]; ok {
		idx.lock.RUnlock()
		return index
	}

	idx.lock.RUnlock()
	if err := idx.ensureIndex(index); err != nil {
		logx.Error(err)
	}
	return index
}

func (idx *Index) ensureIndex(index string) error {
	_, err := idx.singleFlight.Do(index, func() (i interface{}, err error) {
		idx.lock.Lock()
		defer idx.lock.Unlock()

		if _, ok := idx.indices[index]; ok {
			return nil, nil
		}

		defer func() {
			if err == nil {
				idx.indices[index] = lang.Placeholder
			}
		}()

		existsService := elastic.NewIndicesExistsService(idx.client)
		existsService.Index([]string{index})
		exist, err := existsService.Do(context.Background())
		if err != nil {
			return nil, err
		}
		if exist {
			return nil, nil
		}

		createService := idx.client.CreateIndex(index)
		if err := fx.DoWithRetry(func() error {
			// is it necessary to check the result?
			_, err := createService.Do(context.Background())
			return err
		}); err != nil {
			return nil, err
		}

		return nil, nil
	})
	return err
}
