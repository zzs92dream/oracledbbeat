package main

import (
	"os"

	"github.com/elastic/beats/libbeat/beat"

	"github.com/odbaeu/oracledbbeat/beater"
)

func main() {
	err := beat.Run("oracledbbeat", "", beater.New)
	if err != nil {
		os.Exit(1)
	}
}
