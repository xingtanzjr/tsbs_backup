package main

import (
	"bufio"
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"github.com/timescale/tsbs/pkg/targets"
	"github.com/timescale/tsbs/pkg/targets/iotdb"

	// "github.com/timescale/tsbs/pkg/targets/iotdb"
	"strings"
)

// iotdbPoint is a single record(row) of data
type iotdbPoint struct {
	db        string
	deviceID  string // the deviceID(path) of this record, e.g. "root.cpu.host_0"
	values    string
	fieldsCnt int
}

// A struct that storages data points
type iotdbBatch struct {
	m          map[string][]string
	rowCnt     uint   // count of records(rows)
	metricsCnt uint64 // total count of all metrics in this batch
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

	// deviceID and rest values of fields
	lineParts := strings.SplitN(line, ",", 4)

	return data.NewLoadedPoint(
		&iotdbPoint{
			db:        lineParts[0],
			deviceID:  lineParts[1],
			values:    lineParts[3],
			fieldsCnt: len(iotdb.GlobalDataTypeMap[lineParts[0]]),
		})
}

func (d *fileDataSource) Headers() *common.GeneratedDataHeaders { return nil }

func (b *iotdbBatch) Len() uint {
	return b.rowCnt
}

func (b *iotdbBatch) Append(item data.LoadedPoint) {
	that := item.Data.(*iotdbPoint)
	b.rowCnt++
	b.metricsCnt += uint64(that.fieldsCnt)
	b.m[that.db+"."+that.deviceID] = append(b.m[that.db+"."+that.deviceID], that.values)
}

type factory struct{}

func (f *factory) New() targets.Batch {
	return &iotdbBatch{
		m:          map[string][]string{},
		rowCnt:     0,
		metricsCnt: 0,
	}
}
