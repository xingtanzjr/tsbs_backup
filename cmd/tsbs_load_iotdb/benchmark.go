package main

import (
	"bufio"
	"github.com/apache/iotdb-client-go/client"
	"github.com/timescale/tsbs/load"
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/targets"
	"github.com/timescale/tsbs/pkg/targets/iotdb"
	"hash"
	"hash/fnv"
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
	value, ok := iotdb.MetricDeviceIdx[p.deviceID]
	if ok {
		return value
	}

	idx := getDeviceIdx(p.deviceID, indexer.maxPartitions)
	iotdb.MetricDeviceIdx[p.deviceID] = idx

	return idx
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

func getDeviceIdx(host string, maxPartitions uint) uint {
	return 0
	//splits := strings.Split(host, ".")
	//db := splits[len(splits)-2]
	//device := splits[len(splits)-1]
	//
	//idx := 0
	//for _, s := range device {
	//	if s == '0' || s == '1' {
	//		idx = idx * 10 + int(s-'0')
	//	}
	//}
	//
	//idx := int(iotdb.AllMetrics[db]) * (int)maxPartitions / 9 + idx % 9
	//
	//return uint(idx % 9)
}
