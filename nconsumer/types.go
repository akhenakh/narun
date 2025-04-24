package nconsumer

import (
	"net/http"
	"sync"
)

// RequestData is the structure sent *from* the gateway *to* the consumer bridge over NATS.
// It contains the necessary information to reconstruct an *http.Request.
type RequestData struct {
	_          struct{} `cbor:",toarray"`
	Method     string
	Path       string // Raw path from URL.Path
	Query      string // Raw query string from URL.RawQuery
	Proto      string // e.g., "HTTP/1.1"
	Headers    http.Header
	Body       []byte
	RemoteAddr string // Optional: Client IP if available
	RequestURI string // Original request URI
	Host       string // Host header or server name
}

// ResponseData is the structure sent *back* from the consumer bridge *to* the gateway over NATS.
type ResponseData struct {
	_          struct{} `cbor:",toarray"`
	StatusCode int
	Headers    http.Header
	Body       []byte
}

// Pooling for RequestData

var requestDataPool = sync.Pool{
	New: func() any {
		// Pre-allocate headers map for slight potential optimization
		return &RequestData{Headers: make(http.Header)}
	},
}

// GetRequestData retrieves a RequestData object from the pool.
func GetRequestData() *RequestData {
	return requestDataPool.Get().(*RequestData)
}

// PutRequestData returns a RequestData object to the pool after resetting it.
func PutRequestData(data *RequestData) {
	if data == nil {
		return
	}
	// Reset fields to zero values
	data.Method = ""
	data.Path = ""
	data.Query = ""
	data.Proto = ""
	// Clear headers map - important to release references
	for k := range data.Headers {
		delete(data.Headers, k)
	}
	data.Body = nil // Release body slice reference
	data.RemoteAddr = ""
	data.RequestURI = ""
	data.Host = ""

	requestDataPool.Put(data)
}

// Pooling for ResponseData
var responseDataPool = sync.Pool{
	New: func() any {
		// Pre-allocate headers map
		return &ResponseData{Headers: make(http.Header)}
	},
}

// GetResponseData retrieves a ResponseData object from the pool.
func GetResponseData() *ResponseData {
	return responseDataPool.Get().(*ResponseData)
}

// PutResponseData returns a ResponseData object to the pool after resetting it.
func PutResponseData(data *ResponseData) {
	if data == nil {
		return
	}
	// Reset fields
	data.StatusCode = 0
	// Clear headers map
	for k := range data.Headers {
		delete(data.Headers, k)
	}
	data.Body = nil // Release body slice reference

	responseDataPool.Put(data)
}
