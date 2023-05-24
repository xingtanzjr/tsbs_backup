package main

import (
	"bufio"
	"bytes"
	"github.com/apache/iotdb-client-go/client"
	"github.com/spaolacci/murmur3"
	"github.com/timescale/tsbs/load"
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/targets"
	"math"
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
	buffer        *bytes.Buffer
	maxPartitions uint
	hashEndGroups []uint32
	cache         map[string]uint
}

func (i *iotdbIndexer) GetIndex(item data.LoadedPoint) uint {
	p := item.Data.(*iotdbPoint)
	idx, ok := i.cache[p.deviceID]
	if ok {
		return idx
	}

	i.buffer.WriteString(p.deviceID)
	hash := murmur3.Sum32WithSeed(i.buffer.Bytes(), 0x12345678)
	i.buffer.Reset()
	for j := 0; j < int(i.maxPartitions); j++ {
		if hash <= i.hashEndGroups[j] {
			idx = uint(j)
			break
		}
	}
	i.cache[p.deviceID] = idx
	return idx
}

func (b *iotdbBenchmark) GetDataSource() targets.DataSource {
	return &fileDataSource{scanner: bufio.NewScanner(load.GetBufferedReader(b.loaderConfig.FileName))}
}

func (b *iotdbBenchmark) GetBatchFactory() targets.BatchFactory {
	return &factory{}
}

func (b *iotdbBenchmark) GetPointIndexer(maxPartitions uint) targets.PointIndexer {
	if maxPartitions > 1 {
		interval := uint32(math.MaxUint32 / maxPartitions)
		hashEndGroups := make([]uint32, maxPartitions)
		for i := 0; i < int(maxPartitions); i++ {
			if i == int(maxPartitions)-1 {
				hashEndGroups[i] = math.MaxUint32
			} else {
				hashEndGroups[i] = interval*uint32(i+1) - 1
			}
		}
		return &iotdbIndexer{buffer: &bytes.Buffer{}, hashEndGroups: hashEndGroups,
			maxPartitions: maxPartitions, cache: map[string]uint{}}
	}
	return &targets.ConstantIndexer{}
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

func getDeviceIdx(host string, maxPartitions int) int {
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
