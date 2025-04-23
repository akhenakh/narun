package nconsumer

import "net/http"

// RequestData is the structure sent *from* the gateway *to* the consumer bridge over NATS.
// It contains the necessary information to reconstruct an *http.Request.
type RequestData struct {
	Method     string      `cbor:"m,omitempty"`
	Path       string      `cbor:"p,omitempty"`  // Raw path from URL.Path
	Query      string      `cbor:"q,omitempty"`  // Raw query string from URL.RawQuery
	Proto      string      `cbor:"pr,omitempty"` // e.g., "HTTP/1.1"
	Headers    http.Header `cbor:"h,omitempty"`
	Body       []byte      `cbor:"b,omitempty"`
	RemoteAddr string      `cbor:"r,omitempty"` // Optional: Client IP if available
	RequestURI string      `cbor:"u,omitempty"` // Original request URI
	Host       string      `cbor:"o,omitempty"` // Host header or server name
}

// ResponseData is the structure sent *back* from the consumer bridge *to* the gateway over NATS.
type ResponseData struct {
	StatusCode int         `cbor:"s,omitempty"`
	Headers    http.Header `cbor:"h,omitempty"`
	Body       []byte      `cbor:"b,omitempty"`
}
