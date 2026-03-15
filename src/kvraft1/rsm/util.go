package rsm

import (
	"encoding/json"
	"log"
	"os"
)

// Debugging
const debug = false
const enablePersist = false

var firstLog = false

func dPrintf(format string, a ...interface{}) {
	if debug {
		if !firstLog {
			if enablePersist {
				f, _ := os.Create("rsm.log")
				log.SetOutput(f)
			}
			firstLog = true
		}
		log.Printf("====DEBUG rsm=== "+format, a...)
	}
}

func stringify(obj any) string {
	jsonBytes, _ := json.Marshal(obj)
	jsonString := string(jsonBytes)
	return jsonString
}
