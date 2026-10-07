package common

import (
	"log/slog"
	"os"
	"strings"
)

type SLogger struct{ l *slog.Logger }

func NewLogger(service string) *SLogger {
	level := slog.LevelInfo
	switch strings.ToLower(os.Getenv("LOG_LEVEL")) {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})
	version := os.Getenv("SERVICE_VERSION")
	if version == "" {
		version = "sluicegate"
	}
	return &SLogger{l: slog.New(h).With("service", service, "version", version)}
}
func (l *SLogger) Info(msg string, args ...any)  { l.l.Info(msg, args...) }
func (l *SLogger) Warn(msg string, args ...any)  { l.l.Warn(msg, args...) }
func (l *SLogger) Error(msg string, args ...any) { l.l.Error(msg, args...) }
func (l *SLogger) Debug(msg string, args ...any) { l.l.Debug(msg, args...) }
