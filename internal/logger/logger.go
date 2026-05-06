// logger using uber zap for logging for json output

package logger

import (
	"fmt"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// this function will returns a json logger at the requested level, all from the log level (warn, info, debug, error)

// panic  if the log level never got

func New(level string) *zap.Logger {

	var l zapcore.Level

	if err := l.UnmarshalText([]byte(level)); err != nil {
		panic(fmt.Sprintf("logger: unknown level %q: %v", level, err))
	}

	cfg := zap.NewProductionConfig()
	cfg.Level = zap.NewAtomicLevelAt(l)
	cfg.EncoderConfig.TimeKey = "ts"
	cfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	log, err := cfg.Build(zap.AddCallerSkip(0))

	if err != nil {
		panic(fmt.Sprintf("logger: failed to build: %v", err))
	}

	return log
}
