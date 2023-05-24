package main

import (
	"bufio"
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"github.com/timescale/tsbs/pkg/targets"
	"github.com/timescale/tsbs/pkg/targets/iotdb"
	"strings"
)

// iotdbPoint is a single record(row) of data
type iotdbPoint struct {
	deviceID  string // the deviceID(path) of this record, e.g. "root.cpu.host_0"
	values    string
	fieldsCnt int
}

// A struct that storages data points
type iotdbBatch struct {
	m       map[string][]string
	rows    uint   // count of records(rows)
	metrics uint64 // total count of all metrics in this batch
}

type fileDataSource struct {
	scanner *bufio.Scanner
}

func (d *fileDataSource) NextItem() data.LoadedPoint {
	ok := d.scanner.Scan()
	if !ok && d.scanner.Err() == nil { // nothing scanned & no error = EOF
		return data.LoadedPoint{}
	} else if !ok {
		return data.LoadedPoint{}
	}

	// return data.NewLoadedPoint(d.scanner.Bytes())

	line := d.scanner.Text()

	lineParts := strings.SplitN(line, ",", 2) // deviceID and rest values of fields

	return data.NewLoadedPoint(
		&iotdbPoint{
			deviceID:  lineParts[0],
			values:    lineParts[1],
			fieldsCnt: len(iotdb.GlobalDataTypeMap[lineParts[0]]),
		})
}

func (d *fileDataSource) Headers() *common.GeneratedDataHeaders { return nil }

func (b *iotdbBatch) Len() uint {
	return b.rows
}

func (b *iotdbBatch) Append(item data.LoadedPoint) {
	that := item.Data.([]byte)
	line := string(that)
	splits := strings.Split(line, ",")

	b.rows++
	b.metrics += uint64(len(splits) - 2)
	// b.m[that.deviceID] = append(b.m[that.deviceID], that.values)
}

type factory struct{}

func (f *factory) New() targets.Batch {
	return &iotdbBatch{
		m:       map[string][]string{},
		rows:    0,
		metrics: 0,
	}
}
