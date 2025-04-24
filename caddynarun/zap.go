package caddynarun

import (
	"context"
	"log/slog"

	// Added for slog.Time support
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// ZapSlogHandler implements slog.Handler by writing records to a zap.Logger.
type ZapSlogHandler struct {
	zapLogger *zap.Logger
	leveler   slog.Leveler // Handles minimum level filtering
	prefix    string       // For WithGroup support (namespace)
	attrs     []zap.Field  // For WithAttrs support (fields added to handler)
}

// NewZapSlogHandler creates a handler that writes slog records using zap.
// It skips 2 caller frames by default (New + Handle) to show the correct source location.
func NewZapSlogHandler(logger *zap.Logger, level slog.Leveler) *ZapSlogHandler {
	if level == nil {
		level = slog.LevelInfo // Default level if nil
	}
	return &ZapSlogHandler{
		// Skip this func and the Handle func frame
		zapLogger: logger.WithOptions(zap.AddCallerSkip(2)),
		leveler:   level,
		attrs:     nil, // Initial attributes are empty
		prefix:    "",  // Initial prefix is empty
	}
}

// Enabled implements slog.Handler.
func (h *ZapSlogHandler) Enabled(_ context.Context, level slog.Level) bool {
	minLevel := h.leveler.Level()
	// Check slog level first, then zap core level
	return level >= minLevel && h.zapLogger.Core().Enabled(slogLevelToZapLevel(level))
}

// Handle implements slog.Handler.
func (h *ZapSlogHandler) Handle(_ context.Context, record slog.Record) error {
	zapLevel := slogLevelToZapLevel(record.Level)

	// Check if the level is enabled before doing any work
	if !h.zapLogger.Core().Enabled(zapLevel) {
		return nil
	}

	// Check log entry against zap logger
	ce := h.zapLogger.Check(zapLevel, record.Message)
	if ce == nil {
		// Should not happen if Core().Enabled() returned true, but double-check
		return nil
	}

	// Allocate space for handler attrs + record attrs + potential namespace
	allFields := make([]zap.Field, 0, len(h.attrs)+record.NumAttrs()+1)

	// Add attributes attached to the handler via WithAttrs
	allFields = append(allFields, h.attrs...)

	// Add attributes from the specific log record
	record.Attrs(func(attr slog.Attr) bool {
		allFields = append(allFields, slogAttrToZapField(attr))
		return true // Continue iterating
	})

	// If a prefix exists (from WithGroup), add it as a namespace field.
	// zap's JSON encoder handles nesting correctly when a Namespace field is present.
	if h.prefix != "" {
		// Write requires zapcore.Field, so create it here
		nsField := zap.Namespace(h.prefix)
		ce.Write(append([]zapcore.Field{nsField}, allFields...)...)
	} else {
		ce.Write(allFields...)
	}

	return nil
}

// WithAttrs implements slog.Handler.
func (h *ZapSlogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	// Create a new handler instance with combined attributes
	newHandler := &ZapSlogHandler{
		zapLogger: h.zapLogger, // Keep the same logger (and caller skip)
		leveler:   h.leveler,
		prefix:    h.prefix,
		// Allocate new slice and copy existing + new attributes
		attrs: make([]zap.Field, 0, len(h.attrs)+len(attrs)),
	}
	newHandler.attrs = append(newHandler.attrs, h.attrs...)
	for _, attr := range attrs {
		newHandler.attrs = append(newHandler.attrs, slogAttrToZapField(attr))
	}
	return newHandler
}

// WithGroup implements slog.Handler.
func (h *ZapSlogHandler) WithGroup(name string) slog.Handler {
	// Create a new handler instance with the new group name (prefix)
	newHandler := &ZapSlogHandler{
		zapLogger: h.zapLogger, // Keep the same logger
		leveler:   h.leveler,
		attrs:     h.attrs, // Carry over existing attributes
	}
	if name == "" { // If group name is empty, it's a no-op according to slog docs
		newHandler.prefix = h.prefix
	} else if h.prefix == "" {
		newHandler.prefix = name
	} else {
		newHandler.prefix = h.prefix + "." + name // Nest groups using dot separator
	}
	return newHandler
}

// Helper to convert slog.Level to zapcore.Level.
func slogLevelToZapLevel(level slog.Level) zapcore.Level {
	switch level {
	case slog.LevelDebug:
		return zapcore.DebugLevel
	case slog.LevelInfo:
		return zapcore.InfoLevel
	case slog.LevelWarn:
		return zapcore.WarnLevel
	case slog.LevelError:
		return zapcore.ErrorLevel
	default:
		// Handle custom levels based on their numeric value
		if level < slog.LevelInfo {
			return zapcore.DebugLevel
		}
		if level < slog.LevelWarn {
			return zapcore.InfoLevel
		}
		if level < slog.LevelError {
			return zapcore.WarnLevel
		}
		return zapcore.ErrorLevel // Treat >= Error as Error
	}
}

// Helper to convert slog.Attr to zap.Field.
func slogAttrToZapField(attr slog.Attr) zap.Field {
	// Resolve the attribute value before converting
	attr.Value = attr.Value.Resolve()

	switch attr.Value.Kind() {
	case slog.KindString:
		return zap.String(attr.Key, attr.Value.String())
	case slog.KindInt64:
		return zap.Int64(attr.Key, attr.Value.Int64())
	case slog.KindUint64:
		return zap.Uint64(attr.Key, attr.Value.Uint64())
	case slog.KindFloat64:
		return zap.Float64(attr.Key, attr.Value.Float64())
	case slog.KindBool:
		return zap.Bool(attr.Key, attr.Value.Bool())
	case slog.KindTime:
		return zap.Time(attr.Key, attr.Value.Time())
	case slog.KindDuration:
		return zap.Duration(attr.Key, attr.Value.Duration())
	case slog.KindGroup:
		// Convert group attributes into a nested zap object/map
		groupAttrs := attr.Value.Group()
		groupFields := make([]zap.Field, 0, len(groupAttrs))
		for _, groupAttr := range groupAttrs {
			groupFields = append(groupFields, slogAttrToZapField(groupAttr))
		}
		// Use zap.Object which takes fields directly for structured logging
		return zap.Object(attr.Key, zapcore.ObjectMarshalerFunc(func(enc zapcore.ObjectEncoder) error {
			for _, f := range groupFields {
				f.AddTo(enc)
			}
			return nil
		}))
	case slog.KindLogValuer: // Should have been resolved by attr.Value.Resolve()
		// Fallback using Any if somehow still a LogValuer
		return zap.Any(attr.Key, attr.Value.Any())
	default: // KindAny, KindBytes, etc.
		return zap.Any(attr.Key, attr.Value.Any())
	}
}
