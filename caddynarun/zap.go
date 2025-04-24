package caddynarun

import (
	"context"
	"log/slog"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type ZapSlogHandler struct {
	zapLogger *zap.Logger
	leveler   slog.Leveler // Handles minimum level
	prefix    string       // For WithGroup support
	attrs     []zap.Field  // For WithAttrs support
}

// NewZapSlogHandler creates a handler that writes slog records using zap.
func NewZapSlogHandler(logger *zap.Logger, level slog.Leveler) *ZapSlogHandler {
	if level == nil {
		level = slog.LevelInfo // Default level
	}
	return &ZapSlogHandler{
		zapLogger: logger.WithOptions(zap.AddCallerSkip(1)), // Skip handler frames
		leveler:   level,
	}
}

// Enabled implements slog.Handler.
func (h *ZapSlogHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.leveler.Level() && h.zapLogger.Core().Enabled(slogLevelToZapLevel(level))
}

// Handle implements slog.Handler.
func (h *ZapSlogHandler) Handle(_ context.Context, record slog.Record) error {
	zapLevel := slogLevelToZapLevel(record.Level)

	// Check if the level is enabled before doing any work
	if !h.Enabled(context.Background(), record.Level) { // Use Enabled method
		return nil
	}

	if ce := h.zapLogger.Check(zapLevel, record.Message); ce != nil {
		// Allocate enough space upfront
		finalFields := make([]zap.Field, 0, record.NumAttrs()+len(h.attrs)+1) // +1 for potential namespace

		// Add attributes attached to the handler via WithAttrs
		finalFields = append(finalFields, h.attrs...)

		// Add attributes from the specific log record
		record.Attrs(func(attr slog.Attr) bool {
			finalFields = append(finalFields, slogAttrToZapField(attr))
			return true
		})

		if h.prefix != "" {
			// If there's a prefix (from WithGroup), create a Namespace field
			// and pass IT FIRST, followed by the actual fields.
			// The zap encoder (like JSONEncoder) will handle nesting based on the Namespace field.
			nsField := zap.Namespace(h.prefix)
			ce.Write(append([]zapcore.Field{nsField}, finalFields...)...) // Prepend namespace field
		} else {
			// No prefix, just write the collected fields directly.
			ce.Write(finalFields...)
		}
	}
	return nil
}

// WithAttrs implements slog.Handler.
func (h *ZapSlogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	newAttrs := make([]zap.Field, 0, len(h.attrs)+len(attrs))
	newAttrs = append(newAttrs, h.attrs...)
	for _, attr := range attrs {
		newAttrs = append(newAttrs, slogAttrToZapField(attr))
	}
	return &ZapSlogHandler{
		zapLogger: h.zapLogger,
		leveler:   h.leveler,
		prefix:    h.prefix,
		attrs:     newAttrs,
	}
}

// WithGroup implements slog.Handler.
func (h *ZapSlogHandler) WithGroup(name string) slog.Handler {
	newPrefix := name
	if h.prefix != "" {
		newPrefix = h.prefix + "." + name // Nest groups
	}
	return &ZapSlogHandler{
		zapLogger: h.zapLogger,
		leveler:   h.leveler,
		prefix:    newPrefix, // Set/nest the group name
		attrs:     h.attrs,   // Carry over existing attributes
	}
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
		// Handle custom levels or treat as Info
		if level < slog.LevelInfo {
			return zapcore.DebugLevel
		}
		if level < slog.LevelWarn {
			return zapcore.InfoLevel
		}
		if level < slog.LevelError {
			return zapcore.WarnLevel
		}
		return zapcore.ErrorLevel
	}
}

// Helper to convert slog.Attr to zap.Field.
func slogAttrToZapField(attr slog.Attr) zap.Field {
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
	case slog.KindGroup: // Handle groups recursively if needed, or flatten
		// Flattening for simplicity here
		groupFields := make([]zap.Field, 0, len(attr.Value.Group()))
		for _, groupAttr := range attr.Value.Group() {
			// Add group prefix to key? Or use zap.Namespace?
			// Let's keep it simple for now and potentially flatten keys
			groupFields = append(groupFields, slogAttrToZapField(groupAttr))
		}
		// This doesn't work directly, zap doesn't have a simple "Group" field type
		// Option 1: Flatten (e.g., group.key) - More complex
		// Option 2: Use zap.Any with a map - Simpler
		groupMap := make(map[string]interface{})
		for _, groupAttr := range attr.Value.Group() {
			groupMap[groupAttr.Key] = groupAttr.Value.Any()
		}
		return zap.Any(attr.Key, groupMap)

	case slog.KindAny:
		fallthrough // Treat Any the same for zap.Any
	default:
		return zap.Any(attr.Key, attr.Value.Any())
	}
}
