package iotdb

import (
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/apache/iotdb-client-go/client"
	"github.com/timescale/tsbs/pkg/data"
)

// Serializer writes a Point in a serialized form for IoTDB
type Serializer struct {
	BasicPath      string // e.g. "root.sg" is basic path of "root.sg.device". default : "root"
	BasicPathLevel int32  // e.g. 0 for "root", 1 for "root.device"
}

// const iotdbTimeFmt = "2006-01-02 15:04:05"

const defaultBufSize = 4096

// Serialize writes Point p to the given Writer w, so it can be
// loaded by the IoTDB loader. The format is CSV with two lines per Point,
// with the first row being the names of fields and the second row being the
// field values.
//
// e.g.,
// deviceID,timestamp,<fieldName1>,<fieldName2>,<fieldName3>,...
// <deviceID>,<timestamp>,<field1>,<field2>,<field3>,...
// datatype,<datatype1>,<datatype2>,<datatype3>,...
// tags,<tagName1>=<tagValue1>,<tagName2>=<tagValue2>,...
//
// deviceID,timestamp,hostname,value
// root.cpu.host_1,1451606400000000000,'host_1',44.0
// datatype,5,2
// tags,region='eu-west-1',datacenter='eu-west-1c',rack='87'
func (s *Serializer) Serialize(p *data.Point, w io.Writer) error {

	hostname := "unknown"
	for i, v := range p.TagValues() {
		if keyStr := string(p.TagKeys()[i]); keyStr == "hostname" {
			hostname = v.(string)
		}
	}

	buf := make([]byte, 0, defaultBufSize)
	buf = append(buf, []byte(fmt.Sprintf("%s,%s,", modifyHostname(string(p.MeasurementName())), hostname))...)
	buf = append(buf, []byte(fmt.Sprintf("%d,", len(p.FieldValues())))...)
	buf = append(buf, []byte(fmt.Sprintf("%d", p.Timestamp().UTC().UnixMilli()))...)

	fieldValues := p.FieldValues()
	for _, v := range fieldValues {
		valueInStrByte, _ := IotdbFormat(v)
		buf = append(buf, ',')
		buf = append(buf, valueInStrByte...)
	}

	buf = append(buf, '\n')
	_, err := w.Write(buf)

	return err
}

// modifyHostnames makes sure IP address can appear in the path.
// Node names in path can NOT contain "." unless enclosing it within either single quote (') or double quote (").
// In this case, quotes are recognized as part of the node name to avoid ambiguity.
func modifyHostname(hostname string) string {
	if strings.Contains(hostname, ".") {
		if !(hostname[:1] == "`" && hostname[len(hostname)-1:] == "`") {
			// not modified yet
			hostname = "`" + hostname + "`"
		}

	}
	return hostname
}

// Utility function for appending various data types to a byte string
func IotdbFormat(v interface{}) ([]byte, client.TSDataType) {
	// treat all integer as int32
	switch v.(type) {
	case uint:
		// return []byte(strconv.FormatInt(int64(v.(uint)), 10)), client.INT32
		return []byte(strconv.FormatInt(int64(v.(uint)), 10)), client.INT64
	case uint32:
		// return []byte(strconv.FormatInt(int64(v.(uint32)), 10)), client.INT32
		return []byte(strconv.FormatInt(int64(v.(uint32)), 10)), client.INT64
	case uint64:
		// return []byte(strconv.FormatInt(int64(v.(uint64)), 10)), client.INT32
		return []byte(strconv.FormatInt(int64(v.(uint64)), 10)), client.INT64
	case int:
		// return []byte(strconv.FormatInt(int64(v.(int)), 10)), client.INT32
		return []byte(strconv.FormatInt(int64(v.(int)), 10)), client.INT64
	case int32:
		// return []byte(strconv.FormatInt(int64(v.(int32)), 10)), client.INT32
		return []byte(strconv.FormatInt(int64(v.(int32)), 10)), client.INT32
	case int64:
		// return []byte(strconv.FormatInt(int64(v.(int64)), 10)), client.INT32
		return []byte(strconv.FormatInt(int64(v.(int64)), 10)), client.INT64
	case float64:
		// Why -1 ?
		// From Golang source on genericFtoa (called by AppendFloat): 'Negative precision means "only as much as needed to be exact."'
		// Using this instead of an exact number for precision ensures we preserve the precision passed in to the function, allowing us
		// to use different precision for different use cases.
		return []byte(strconv.FormatFloat(float64(v.(float64)), 'f', -1, 64)), client.DOUBLE
	case float32:
		return []byte(strconv.FormatFloat(float64(v.(float32)), 'f', -1, 32)), client.FLOAT
	case bool:
		return []byte(strconv.FormatBool(v.(bool))), client.BOOLEAN
	case string:
		return []byte(v.(string)), client.TEXT
	case nil:
		return []byte(v.(string)), client.UNKNOWN
	default:
		panic(fmt.Sprintf("unknown field type for %#v", v))
	}
}
