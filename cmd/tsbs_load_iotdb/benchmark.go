package main

import (
	"bufio"
	"github.com/timescale/tsbs/pkg/data"
	"hash"
	"hash/fnv"

	"github.com/apache/iotdb-client-go/client"
	"github.com/timescale/tsbs/load"
	"github.com/timescale/tsbs/pkg/targets"
)

func newBenchmark(clientConfig client.Config, loaderConfig load.BenchmarkRunnerConfig) targets.Benchmark {
	return &iotdbBenchmark{
		clientConfig:   clientConfig,
		loaderConfig:   loaderConfig,
		recordsMaxRows: recordsMaxRows,
	}
}

type iotdbBenchmark struct {
	clientConfig   client.Config
	loaderConfig   load.BenchmarkRunnerConfig
	recordsMaxRows int
}

type iotdbIndexer struct {
	maxPartitions uint
	hasher        hash.Hash32
}

func (indexer *iotdbIndexer) GetIndex(item data.LoadedPoint) uint {
	p := item.Data.(*iotdbPoint)
	indexer.hasher.Reset()
	_, err := indexer.hasher.Write([]byte(p.deviceID))
	if err != nil {
		return 0
	}
	return uint(indexer.hasher.Sum32()) % indexer.maxPartitions
}

func (b *iotdbBenchmark) GetDataSource() targets.DataSource {
	return &fileDataSource{scanner: bufio.NewScanner(load.GetBufferedReader(b.loaderConfig.FileName))}
}

func (b *iotdbBenchmark) GetBatchFactory() targets.BatchFactory {
	return &factory{}
}

func (b *iotdbBenchmark) GetPointIndexer(maxPartitions uint) targets.PointIndexer {
	return &iotdbIndexer{maxPartitions: maxPartitions, hasher: fnv.New32a()}
}

func (b *iotdbBenchmark) GetProcessor() targets.Processor {
	return &processor{
		recordsMaxRows:       b.recordsMaxRows,
		loadToSCV:            loadToSCV,
		csvFilepathPrefix:    csvFilepathPrefix,
		useAlignedTimeseries: useAlignedTimeseries,
		storeTags:            storeTags,
	}
}

func (b *iotdbBenchmark) GetDBCreator() targets.DBCreator {
	return &dbCreator{
		loadToSCV: loadToSCV,
	}
}
