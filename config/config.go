// Config is put into a different package to prevent cyclic imports in case
// it is needed in several locations

package config

import "time"

// Config ...
type Config struct {
	Period  time.Duration `config:"period"`
	OciURLs []string      `config:"OciURLs"`
}

// DefaultConfig ...
var DefaultConfig = Config{
	Period:  10 * time.Second,
	OciURLs: nil,
}
