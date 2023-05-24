package main

import (
	"bufio"
	"fmt"
	"github.com/timescale/tsbs/pkg/targets/iotdb"
	"strconv"
	"strings"
	"time"

	"github.com/apache/iotdb-client-go/client"
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"github.com/timescale/tsbs/pkg/targets"
)

// iotdbPoint is a single record(row) of data
type iotdbPoint struct {
	deviceID     string // the deviceID(path) of this record, e.g. "root.cpu.host_0"
	timestamp    int64
	measurements []string
	values       []interface{}
	dataTypes    []client.TSDataType
	tagString    string

	fieldsCnt uint64
}

func (p *iotdbPoint) generateTagsAttributesSQL() string {
	if p.tagString == "" {
		// no tags for this host. This is not a normal behavior in benchmark.
		return fmt.Sprintf("CREATE timeseries %s._tags with datatype=INT32, encoding=RLE, compression=SNAPPY", p.deviceID)
	}
	sql := "CREATE timeseries %s._tags with datatype=INT32, encoding=RLE, compression=SNAPPY attributes(%s)"
	// sql2 := "ALTER timeseries %s._tags UPSERT attributes(%s)"
	return fmt.Sprintf(sql, p.deviceID, p.tagString)
}

// parse datatype and convert string into interface
func parseDataToInterface(datatype client.TSDataType, str string) (interface{}, error) {
	switch client.TSDataType(datatype) {
	case client.BOOLEAN:
		value, err := strconv.ParseBool(str)
		return interface{}(value), err
	case client.INT32:
		value, err := strconv.ParseInt(str, 10, 32)
		return interface{}(int32(value)), err
	case client.INT64:
		value, err := strconv.ParseInt(str, 10, 64)
		return interface{}(int64(value)), err
	case client.FLOAT:
		value, err := strconv.ParseFloat(str, 32)
		return interface{}(float32(value)), err
	case client.DOUBLE:
		value, err := strconv.ParseFloat(str, 64)
		return interface{}(float64(value)), err
	case client.TEXT:
		return interface{}(str), nil
	case client.UNKNOWN:
		return interface{}(nil), fmt.Errorf("datatype client.UNKNOW, value:%s", str)
	default:
		return interface{}(nil), fmt.Errorf("unknown datatype, value:%s", str)
	}
}

type fileDataSource struct {
	scanner *bufio.Scanner
}

func parseLine(line string) data.LoadedPoint {
	lineParts := strings.Split(line, ",") // deviceID and rest values of fields
	deviceID := lineParts[0]
	deviceType := strings.Split(deviceID, ".")[2]

	dataTypes := iotdb.GlobalDataTypeMap[deviceType]
	measurements := iotdb.GlobalMeasurementMap[deviceType]

	timestamp, err := strconv.ParseInt(lineParts[1], 10, 64)
	if err != nil {
		fatal("timestamp convert err: %v", err)
	}

	timestamp = timestamp / int64(time.Millisecond)

	var values []interface{}

	for idx := 2; idx < len(lineParts); idx++ {
		value, err := parseDataToInterface(dataTypes[idx-2], lineParts[idx])
		if err != nil {
			panic(fmt.Errorf("iotdb fileDataSource NextItem Parse error:%v", err))
		}
		values = append(values, value)
	}

	return data.NewLoadedPoint(
		&iotdbPoint{
			deviceID:     lineParts[0],
			timestamp:    timestamp,
			measurements: measurements,
			values:       values,
			dataTypes:    dataTypes,
			tagString:    "",
			fieldsCnt:    uint64(len(measurements)),
		})
}

func (d *fileDataSource) NextItem() data.LoadedPoint {
	ok := d.scanner.Scan()
	if !ok && d.scanner.Err() == nil { // nothing scanned & no error = EOF
		return data.LoadedPoint{}
	} else if !ok {
		return data.LoadedPoint{}
	}
	line := d.scanner.Text()

	return parseLine(line)
}

func (d *fileDataSource) Headers() *common.GeneratedDataHeaders { return nil }

// A struct that storages data points
type iotdbBatch struct {
	points  []*iotdbPoint
	rows    uint   // count of records(rows)
	metrics uint64 // total count of all metrics in this batch
}

func (b *iotdbBatch) Len() uint {
	return b.rows
}

func (b *iotdbBatch) Append(item data.LoadedPoint) {
	b.rows++
	b.points = append(b.points, item.Data.(*iotdbPoint))
	b.metrics += item.Data.(*iotdbPoint).fieldsCnt
}

type factory struct{}

func (f *factory) New() targets.Batch {
	return &iotdbBatch{
		rows:    0,
		metrics: 0,
	}
}
