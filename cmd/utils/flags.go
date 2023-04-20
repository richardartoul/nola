package utils

import (
	"fmt"
	"net"
	"os"
	"strconv"

	"golang.org/x/exp/slog"
)

func ParseLog(logLevel, logFormat string) (*slog.Logger, error) {
	var logHandlerOpts slog.HandlerOptions
	switch logLevel {
	case "info":
		logHandlerOpts = slog.HandlerOptions{Level: slog.LevelInfo}
	case "debug":
		logHandlerOpts = slog.HandlerOptions{Level: slog.LevelDebug}
	case "warn":
		logHandlerOpts = slog.HandlerOptions{Level: slog.LevelWarn}
	case "error":
		logHandlerOpts = slog.HandlerOptions{Level: slog.LevelError}
	default:
		return nil, fmt.Errorf("invalid log level: %s", logLevel)
	}

	switch logFormat {
	case "json":
		return slog.New(logHandlerOpts.NewJSONHandler(os.Stdout)), nil
	case "text":
		return slog.New(logHandlerOpts.NewTextHandler(os.Stdout)), nil
	default:
		return nil, fmt.Errorf("invalid log format: %s", logFormat)
	}
}

func ParsePortFromAddr(addr string) (int, error) {
	_, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return 0, err
	}

	return strconv.Atoi(portStr)
}

func ParseHostFromAddr(addr string) (string, error) {
	host, _, err := net.SplitHostPort(addr)
	return host, err
}
