// operator

package ghbase

import (
	"errors"
	"fmt"

	//"github.com/alecthomas/log4go"
	"github.com/tsuna/gohbase"
	"github.com/tsuna/gohbase/filter"
	"github.com/tsuna/gohbase/hrpc"
	"golang.org/x/net/context"
)

type HBase struct {
	client *Client
}

var hbaseConn *HBase

func InitHbase(zkQuorum string, option ...gohbase.Option) {
	hbaseConn = &HBase{
		client: NewClient(zkQuorum, option...),
	}
	fmt.Println("Hbase:" + zkQuorum + " Connection Established! ")

}

func Gets(table, rowKey, colFam, qualifier string) (str string, err error) {
	family := map[string][]string{colFam: {qualifier}}
	getRequest, err := hrpc.NewGetStr(context.Background(), table, rowKey, hrpc.Families(family))
	if err != nil {
		fmt.Println("hrpc.NewGetStr: ", err.Error())
	}
	res, err := hbaseConn.client.Get(context.Background(), getRequest)
	if err != nil {
		fmt.Println("hbase clients: ", err.Error())
	}
	defer func() {
		if errs := recover(); errs != nil {
			err = fmt.Errorf("%v", errs)
			switch fmt.Sprintf("%v", errs) {
			case "runtime error: index out of range":
				err = errors.New("NoSuchRowKeyOrQualifierException")
			case "invalid memory address or nil pointer dereference":
				err = errors.New("NoSuchColFamilyException")
			default:
				err = fmt.Errorf("%v", errs)
			}
		}
	}()
	return string(res.Cells[0].Value), nil
}

func Puts(table, rowKey, colFam string, colVal map[string][]byte) (res *hrpc.Result, err error) {

	values := map[string]map[string][]byte{colFam: colVal}
	putRequest, err := hrpc.NewPutStr(context.Background(), table, rowKey, values)
	if err != nil {
		fmt.Println(err.Error())
		return &hrpc.Result{}, err
	}

	_, err = hbaseConn.client.Put(context.Background(), putRequest)
	if err != nil {
		fmt.Println(err.Error())
		return &hrpc.Result{}, err
	}

	return res, nil

}

func ScanByRowKey(table, startRow, stopRow, colFam, qualifier string) error {
	family := map[string][]string{colFam: {qualifier}}
	scan, err := hrpc.NewScanRangeStr(context.Background(), table, startRow, stopRow, hrpc.Families(family))
	if err != nil {
		fmt.Println(err.Error())
		return err
	}

	scanner := hbaseConn.client.Scan(context.Background(), scan)

	for {
		res, err := scanner.Next()

		if err != nil {
			fmt.Println(err.Error())
			return err
		}

		fmt.Println("row: ", string(res.Cells[0].Row), "       val: ", string(res.Cells[0].Value))
	}

	return nil
}

func ColumnValueFilter(table, colFam, column, value string) {
	sFilter := filter.NewSingleColumnValueFilter([]byte(colFam), []byte(column), filter.Equal,
		filter.NewSubstringComparator(value), true, true)
	scanRequest, err := hrpc.NewScanStr(context.Background(), table, hrpc.Filters(sFilter))
	if err != nil {
		fmt.Println("new scan err: ", err)
	}
	scanner := hbaseConn.client.Scan(context.Background(), scanRequest)

	for {
		res, err := scanner.Next()

		if err != nil {
			fmt.Println(err.Error())
			return
		}

		fmt.Println("row: ", string(res.Cells[0].Row), "       val: ", string(res.Cells[0].Value))
	}
}
