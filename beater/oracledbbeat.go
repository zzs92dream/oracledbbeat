package beater

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/publisher"

	_ "github.com/mattn/go-oci8" // Oracle OCI driver
	"github.com/odbaeu/oracledbbeat/config"
)

// Oracledbbeat ...
type Oracledbbeat struct {
	done          chan struct{}
	config        config.Config
	client        publisher.Client
	lastIndexTime map[string]time.Time
}

// struct for defined beat metrics
type metric struct {
	name string
	sql  string
}

// Definition of all metrics of this beat
var (
	metrics = []metric{
		metric{
			name: "oracledb_status",
			// sql:  "SELECT 1267156488496 dbid, instance_name, status, sysdate jetzt FROM v$instance",
			sql: "SELECT (SELECT dbid FROM v$database) dbid, instance_name, status FROM v$instance",
		},
	}
)

// New Creates beater
func New(b *beat.Beat, cfg *common.Config) (beat.Beater, error) {
	config := config.DefaultConfig
	if err := cfg.Unpack(&config); err != nil {
		return nil, fmt.Errorf("Error reading config file: %v", err)
	}

	bt := &Oracledbbeat{
		done:   make(chan struct{}),
		config: config,
	}
	return bt, nil
}

// Run runs beater
func (bt *Oracledbbeat) Run(b *beat.Beat) error {
	logp.Info("oracledbbeat is running! Hit CTRL-C to stop it.")

	bt.client = b.Publisher.Connect()
	ticker := time.NewTicker(bt.config.Period)
	// counter := 1
	for {

		select {
		case <-bt.done:
			return nil
		case <-ticker.C:
		}

		err := bt.gather(b) // call gather
		if err != nil {
			logp.Err("Error during gather:", err)
		}
		//		counter++
	}
}

// Stop stops beater
func (bt *Oracledbbeat) Stop() {
	bt.client.Close()
	close(bt.done)
}

func (bt *Oracledbbeat) gather(b *beat.Beat) error {
	// NLS_LANG is set to American format. At least NLS_NUMERIC_CHARACTERS has to be ".,".
	os.Setenv("NLS_LANG", "AMERICAN_AMERICA.AL32UTF8")
	// os.Setenv("NLS_DATE_FORMAT", "YYYY-MM-DD\"T\"HH24:MI:SS")
	var (
		err error
		db  *sql.DB
	)
	// TODO: just for debug... Remove following line.
	if bt.config.OciURLs == nil {
		bt.config.OciURLs = append(bt.config.OciURLs, os.Getenv("DATA_SOURCE_NAME"))
	}

	// Start a dbworker process for each configured database
	for _, ociURL := range bt.config.OciURLs {
		go func(ociURL string) {
			db, err = sql.Open("oci8", ociURL)
			if err != nil {
				log.Println("Error opening connection to database:", err)
				return
			}
			defer db.Close()

			// call dbworker
			err := bt.dbworker(b, db)
			if err != nil {
				log.Println("Error in dbworker:", err)
				return
			}
		}(ociURL)
	}

	return nil
}

func (bt *Oracledbbeat) dbworker(b *beat.Beat, db *sql.DB) error {
	for _, m := range metrics {
		err := bt.processMetric(b, db, m)
		if err != nil {
			logp.Err("Error processing metric '"+m.name+"'", err)
		}
	}
	return nil
}

func (bt *Oracledbbeat) processMetric(b *beat.Beat, db *sql.DB, m metric) error {
	// Run query
	queryResult, err := db.Query(m.sql)
	if err != nil {
		return err
	}
	defer queryResult.Close()

	// pointer on data of a database column
	rowData := map[string]*interface{}{}
	// holds data of a database column
	rowVars := []interface{}{}
	// holds all column names (always upper case, oracle related)
	colNames, err := queryResult.Columns()
	// bring poniters in place
	for _, col := range colNames {
		rowData[col] = new(interface{})
		rowVars = append(rowVars, rowData[col])
	}

	// loop through database result set
	for queryResult.Next() {
		if err := queryResult.Scan(rowVars...); err != nil {
			return err
		}

		// Define header of MapStr for beat message
		row := common.MapStr{
			"@timestamp": common.Time(time.Now()),
			"type":       m.name,
		}
		// parse row (also sends beat message)
		err := bt.parseRow(rowData, row, m)
		if err != nil {
			logp.Err("Error during parseRow function:", err)
		}
	}

	return nil
}

func (bt *Oracledbbeat) parseRow(rowData map[string]*interface{}, row common.MapStr, m metric) error {
	for k, v := range rowData {
		if v == nil {
			continue
		}

		// Convert *interface{} to common.MapStr.
		switch val := (*v).(type) {
		case string:
			// Oracle's NUMBER seems not to be recognized correctly.
			// Therefore we convert int and float back to it's real type...
			if vv, err := strconv.ParseInt(val, 10, 64); err == nil {
				// fmt.Println("string-int", k, *v)
				row[k] = vv
			} else if vv, err := strconv.ParseFloat(val, 64); err == nil {
				// fmt.Println("string-float", k, *v)
				row[k] = vv
			} else {
				// fmt.Println("string", k, *v)
				row[k] = val
			}
		case int64, int32, int:
			row[k] = val
			// fmt.Println("int", k, *v)
		case []byte:
			row[k] = string(val)
			// fmt.Println("byte", k, *v)
		case time.Time:
			row[k] = common.Time(val)
			// fmt.Println("Time", k, *v)
		default:
			logp.Err("Failed to convert column to golang datatype. Key:", k, "- Value:", val)
		}
	}
	// publish event
	if ok := bt.client.PublishEvent(row); !ok {
		logp.Err("Error: failed publishing event.")
	} else {
		logp.Info("Successful published event:", row)
		fmt.Println("INFO: Successful published event:", row)
	}

	return nil
}
