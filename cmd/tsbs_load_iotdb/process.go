package main

import (
	"fmt"
	"github.com/timescale/tsbs/pkg/targets/iotdb"
	"strings"

	//"github.com/timescale/tsbs/pkg/data"
	"os"
	//"strconv"
	//"strings"
	//"time"

	"github.com/apache/iotdb-client-go/client"
	"github.com/timescale/tsbs/pkg/targets"
	//"github.com/timescale/tsbs/pkg/targets/iotdb"
)

type processor struct {
	numWorker                int // the worker(like thread) ID of this processor
	session                  client.Session
	recordsMaxRows           int             // max rows of records in 'InsertRecords'
	ProcessedTagsDeviceIDMap map[string]bool // already processed device ID

	loadToSCV         bool                // if true, do NOT insert into databases, but generate csv files instead.
	csvFilepathPrefix string              // Prefix of filepath for csv files. Specific a folder or a folder with filename prefix.
	filePtrMap        map[string]*os.File // file pointer for each deviceID

	useAlignedTimeseries bool // using aligned timeseries if set true.
	storeTags            bool // store tags if set true. Can NOT be used if useAlignedTimeseries is set true.
}

func (p *processor) Init(numWorker int, doLoad, hashWorkers bool) {
	p.numWorker = numWorker
	if !doLoad {
		return
	}
	if p.loadToSCV {
		p.filePtrMap = make(map[string]*os.File)
	} else {
		p.ProcessedTagsDeviceIDMap = make(map[string]bool)
		p.session = client.NewSession(&clientConfig)
		if err := p.session.Open(false, timeoutInMs); err != nil {
			errMsg := fmt.Sprintf("IoTDB processor init error, session is not open: %v, ", err)
			errMsg = errMsg + fmt.Sprintf("timeout setting: %d ms\n", timeoutInMs)
			fatal(errMsg)
		}
	}
}

type records struct {
	deviceIds    []string
	measurements [][]string
	dataTypes    [][]client.TSDataType
	values       [][]interface{}
	timestamps   []int64
}

//func (p *processor) pointsToRecords(points []*iotdbPoint) (records, []string) {
//	var rcds records
//	// var sqlList []string = nil
//	for _, row := range points {
//		rcds.deviceIds = append(rcds.deviceIds, row.deviceID)
//		rcds.measurements = append(rcds.measurements, row.measurements)
//		//rcds.dataTypes = append(rcds.dataTypes, row.dataTypes)
//		rcds.values = append(rcds.values, row.values)
//		rcds.timestamps = append(rcds.timestamps, row.timestamp)
//	}
//	return rcds, nil
//}

//
//func minInt(x int, y int) int {
//	if x < y {
//		return x
//	}
//	return y
//}
//
//func generateCSVContent(point *iotdbPoint) string {
//	var valueList []string
//	valueList = append(valueList, strconv.FormatInt(point.timestamp, 10))
//	for _, value := range point.values {
//		valueInStrByte, _ := iotdb.IotdbFormat(value)
//		valueList = append(valueList, string(valueInStrByte))
//	}
//	content := strings.Join(valueList, ",")
//	content += "\n"
//	return content
//}

func (p *processor) ProcessBatch(b targets.Batch, doLoad bool) (metricCount, rowCount uint64) {
	batch := b.(*iotdbBatch)

	if !doLoad {
		return batch.metricsCnt, uint64(batch.rowCnt)
	}

	if !p.loadToSCV {
		// TODO move code
	}


	for device, values := range batch.m {
		db := strings.Split(device, ".")[0]
		tablet, err := client.NewTablet("root" + device, iotdb.GlobalMeasurementMap[db], len(values))

		for
	}

	//
	//// insert records into the database
	//for index := 0; index < len(batch.points); {
	//	startIndex := index
	//	var endIndex int
	//	if p.recordsMaxRows > 0 {
	//		endIndex = minInt(len(batch.points), index+p.recordsMaxRows)
	//	} else {
	//		endIndex = len(batch.points)
	//	}
	//	rcds, tempSqlList := p.pointsToRecords(batch.points[startIndex:endIndex])
	//
	//	// using relative API according to "aligned-timeseries" setting
	//	var err error
	//	if p.useAlignedTimeseries {
	//		_, err = p.session.InsertAlignedRecords(
	//			rcds.deviceIds, rcds.measurements, rcds.dataTypes, rcds.values, rcds.timestamps,
	//		)
	//	} else {
	//		_, err = p.session.InsertRecords(
	//			rcds.deviceIds, rcds.measurements, rcds.dataTypes, rcds.values, rcds.timestamps,
	//		)
	//	}
	//	if err != nil {
	//		fatal("ProcessBatch error:%v", err)
	//	}
	//	index = endIndex
	//}

	metricCount = batch.metricsCnt
	rowCount = uint64(batch.rowCnt)
	return metricCount, rowCount
}

//func parseLine(line string) data.LoadedPoint {
//	lineParts := strings.Split(line, ",") // deviceID and rest values of fields
//	deviceID := lineParts[0]
//	deviceType := strings.Split(deviceID, ".")[2]
//
//	dataTypes := iotdb.GlobalDataTypeMap[deviceType]
//	measurements := iotdb.GlobalMeasurementMap[deviceType]
//
//	timestamp, err := strconv.ParseInt(lineParts[1], 10, 64)
//	if err != nil {
//		fatal("timestamp convert err: %v", err)
//	}
//
//	timestamp = timestamp / int64(time.Millisecond)
//
//	var values []interface{}
//
//	for idx := 2; idx < len(lineParts); idx++ {
//		value, err := parseDataToInterface(dataTypes[idx-2], lineParts[idx])
//		if err != nil {
//			panic(fmt.Errorf("iotdb fileDataSource NextItem Parse error:%v", err))
//		}
//		values = append(values, value)
//	}
//
//	return data.NewLoadedPoint(
//		&iotdbPoint{
//			deviceID:  lineParts[0],
//			timestamp: timestamp,
//			values:    values,
//			fieldsCnt: uint64(len(measurements)),
//		})
//}

//// parse datatype and convert string into interface
//func parseDataToInterface(datatype client.TSDataType, str string) (interface{}, error) {
//	switch client.TSDataType(datatype) {
//	case client.BOOLEAN:
//		value, err := strconv.ParseBool(str)
//		return interface{}(value), err
//	case client.INT32:
//		value, err := strconv.ParseInt(str, 10, 32)
//		return interface{}(int32(value)), err
//	case client.INT64:
//		value, err := strconv.ParseInt(str, 10, 64)
//		return interface{}(int64(value)), err
//	case client.FLOAT:
//		value, err := strconv.ParseFloat(str, 32)
//		return interface{}(float32(value)), err
//	case client.DOUBLE:
//		value, err := strconv.ParseFloat(str, 64)
//		return interface{}(float64(value)), err
//	case client.TEXT:
//		return interface{}(str), nil
//	case client.UNKNOWN:
//		return interface{}(nil), fmt.Errorf("datatype client.UNKNOW, value:%s", str)
//	default:
//		return interface{}(nil), fmt.Errorf("unknown datatype, value:%s", str)
//	}
//}
