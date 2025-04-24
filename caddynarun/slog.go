package caddynarun

import (
	"fmt"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
	"go.uber.org/zap"
)

// slog Wrapper for natsutil
// natsutil expects slog, Caddy provides zap. Create a simple wrapper.

type SlogWrapper struct {
	logger *zap.Logger
}

// NewSlogWrapper creates a slog-compatible wrapper around a zap logger.
func NewSlogWrapper(zapLogger *zap.Logger) *SlogWrapper {
	// Use the original logger, but maybe skip one caller frame if SetupJetStream logs look weird
	// return &SlogWrapper{logger: zapLogger.WithOptions(zap.AddCallerSkip(1))}
	return &SlogWrapper{logger: zapLogger}
}

// Convert slog key-value pairs to zap fields.
func convertSlogArgsToZapFields(args ...any) []zap.Field {
	fields := make([]zap.Field, 0, len(args)/2)
	for i := 0; i < len(args); i += 2 {
		key, ok := args[i].(string)
		if !ok {
			// Handle error: slog arguments should be key-value pairs with string keys
			// For simplicity, we'll just create a field indicating the problem
			fields = append(fields, zap.Any(fmt.Sprintf("invalid_slog_key_at_index_%d", i), args[i]))
			if i+1 < len(args) { // Also log the value if it exists
				fields = append(fields, zap.Any(fmt.Sprintf("invalid_slog_value_at_index_%d", i+1), args[i+1]))
			}
			continue // Skip this pair
		}
		if i+1 >= len(args) {
			// Handle error: odd number of arguments, missing value for key
			fields = append(fields, zap.Any(fmt.Sprintf("missing_slog_value_for_key_%s", key), "(no value)"))
			break // Stop processing args
		}
		value := args[i+1]
		fields = append(fields, zap.Any(key, value)) // zap.Any handles various types
	}
	return fields
}

// Info implements the slog interface.
func (s *SlogWrapper) Info(msg string, args ...any) {
	if ce := s.logger.Check(zap.InfoLevel, msg); ce != nil {
		ce.Write(convertSlogArgsToZapFields(args...)...)
	}
}

// Warn implements the slog interface.
func (s *SlogWrapper) Warn(msg string, args ...any) {
	if ce := s.logger.Check(zap.WarnLevel, msg); ce != nil {
		ce.Write(convertSlogArgsToZapFields(args...)...)
	}
}

// Error implements the slog interface.
func (s *SlogWrapper) Error(msg string, args ...any) {
	if ce := s.logger.Check(zap.ErrorLevel, msg); ce != nil {
		ce.Write(convertSlogArgsToZapFields(args...)...)
	}
}

// Debug implements the slog interface.
func (s *SlogWrapper) Debug(msg string, args ...any) {
	if ce := s.logger.Check(zap.DebugLevel, msg); ce != nil {
		ce.Write(convertSlogArgsToZapFields(args...)...)
	}
}

// --- Interface Guards ---
var (
	_ caddy.Module                = (*Handler)(nil)
	_ caddy.Provisioner           = (*Handler)(nil)
	_ caddy.Validator             = (*Handler)(nil)
	_ caddy.CleanerUpper          = (*Handler)(nil)
	_ caddyhttp.MiddlewareHandler = (*Handler)(nil)
	_ caddyfile.Unmarshaler       = (*Handler)(nil)
)

// Compile-time check that SlogWrapper implements the needed methods.
// Define an interface matching the methods natsutil.SetupJetStream actually uses from slog.
type natsutilSlogLogger interface {
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
	Debug(msg string, args ...any) // Include Debug if natsutil uses it
}

var (
	_ natsutilSlogLogger = (*SlogWrapper)(nil)
)
