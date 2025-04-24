package nconsumer

import (
	"bytes"
	"net/http"
)

// responseWriterShim implements http.ResponseWriter to capture the response
// written by the user's handler.
type responseWriterShim struct {
	header      http.Header
	body        *bytes.Buffer
	statusCode  int
	wroteHeader bool
}

func newResponseWriterShim() *responseWriterShim {
	return &responseWriterShim{
		header: make(http.Header),
		body:   new(bytes.Buffer),
		// Default status code if WriteHeader is not called
		statusCode: http.StatusOK,
	}
}

func (rw *responseWriterShim) Header() http.Header {
	return rw.header
}

func (rw *responseWriterShim) Write(buf []byte) (int, error) {
	if !rw.wroteHeader {
		rw.WriteHeader(http.StatusOK) // Default if not explicitly set
	}
	return rw.body.Write(buf)
}

func (rw *responseWriterShim) WriteHeader(statusCode int) {
	if rw.wroteHeader {
		// Maybe log a warning here: "multiple WriteHeader calls"
		return
	}
	rw.statusCode = statusCode
	rw.wroteHeader = true
}

// copyTo copies the captured response data into a target ResponseData object.
// It performs necessary copies to avoid aliasing issues with pooled objects.
func (rw *responseWriterShim) copyTo(target *ResponseData) {
	target.StatusCode = rw.statusCode

	// Deep copy headers
	for k, v := range rw.header {
		// Create a copy of the slice
		vc := make([]string, len(v))
		copy(vc, v)
		target.Headers[k] = vc
	}

	// Copy body bytes. rw.body.Bytes() already returns a copy.
	if rw.body.Len() > 0 {
		target.Body = rw.body.Bytes()
	} else {
		target.Body = nil // Ensure it's nil if empty
	}
}
