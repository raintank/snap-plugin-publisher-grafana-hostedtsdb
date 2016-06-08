package main

import (
	"os"

	"github.com/intelsdi-x/snap/control/plugin"
	"github.com/raintank/snap-plugin-publisher-grafananet-tsdb/hostedtsdb"
)

func main() {
	// Define metadata about Plugin
	meta := hostedtsdb.Meta()

	// Start a collector
	plugin.Start(meta, new(hostedtsdb.HostedtsdbPublisher), os.Args[1])
}
