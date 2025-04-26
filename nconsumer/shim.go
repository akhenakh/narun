// narun/nconsumer/shim.go
package nconsumer

import (
	"bytes"
	"net/http"
	"sync" // Import sync package
)

// Pool for response body buffers to reduce allocations.
var bufferPool = sync.Pool{
	New: func() any {
		// Preallocate slightly to avoid initial growth for common small responses
		return bytes.NewBuffer(make([]byte, 0, 4096))
	},
}

// responseWriterShim implements http.ResponseWriter to capture the response
// written by the user's handler. It now uses a pooled buffer.
type responseWriterShim struct {
	header      http.Header
	body        *bytes.Buffer // Will be obtained from bufferPool
	statusCode  int
	wroteHeader bool
}

// Reset prepares the shim for reuse by clearing headers, resetting the buffer,
// and resetting status/flags. It returns the buffer to its pool.
func (rw *responseWriterShim) Reset() {
	// Clear headers efficiently. Iterating and deleting is needed for maps.
	for k := range rw.header {
		delete(rw.header, k)
	}

	// Reset the buffer and return it to the pool
	if rw.body != nil {
		rw.body.Reset()
		bufferPool.Put(rw.body)
		rw.body = nil // Avoid holding onto the pooled buffer instance directly
	}

	// Reset status and flags
	rw.statusCode = http.StatusOK // Reset to default
	rw.wroteHeader = false
}

func (rw *responseWriterShim) Header() http.Header {
	// Lazily initialize header map if needed (might be cleared by Reset)
	// This shouldn't strictly be necessary if the pool's New func initializes it.
	if rw.header == nil {
		rw.header = make(http.Header)
	}
	return rw.header
}

func (rw *responseWriterShim) Write(buf []byte) (int, error) {
	if !rw.wroteHeader {
		rw.WriteHeader(http.StatusOK) // Default if not explicitly set
	}
	// Ensure body buffer exists (it should if obtained from pool correctly)
	if rw.body == nil {
		// This indicates an issue with pooling logic, get a new one as fallback
		rw.body = bufferPool.Get().(*bytes.Buffer)
	}
	return rw.body.Write(buf)
}

func (rw *responseWriterShim) WriteHeader(statusCode int) {
	if rw.wroteHeader {
		return
	}
	// Ensure header map exists
	if rw.header == nil {
		rw.header = make(http.Header)
	}
	rw.statusCode = statusCode
	rw.wroteHeader = true
}
