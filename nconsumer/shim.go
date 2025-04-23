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

// Export captured data
func (rw *responseWriterShim) getResponseData() *ResponseData {
	return &ResponseData{
		StatusCode: rw.statusCode,
		Headers:    rw.header,
		Body:       rw.body.Bytes(),
	}
}
