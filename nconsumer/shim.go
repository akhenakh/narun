package nconsumer

import (
	"bytes"
	"net/http"

	v1 "github.com/akhenakh/narun/gen/go/httprpc/v1"
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

// copyToProto copies the captured response data into a target NatsHttpResponse object.
// It uses the proto setters (Opaque API style).
func (rw *responseWriterShim) copyToProto(target *v1.NatsHttpResponse) {
	target.SetStatusCode(int32(rw.statusCode))

	headers := make(map[string]*v1.HeaderValues)
	for key, values := range rw.header {
		if len(values) > 0 {
			headerVal := &v1.HeaderValues{}
			headerVal.SetValues(values)
			headers[key] = headerVal
		}
	}
	target.SetHeaders(headers)

	if rw.body.Len() > 0 {
		target.SetBody(rw.body.Bytes())
	} else {
		target.SetBody(nil)
	}
}
