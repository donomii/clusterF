package httpclient

import (
	"context"
	"io"
	"log"
	"net/http"
	"strings"
)

// RequestOption customizes an outgoing HTTP request.
type RequestOption func(*http.Request) error

// WithHeader sets a request header key/value pair.
func WithHeader(key, value string) RequestOption {
	return func(req *http.Request) error {
		req.Header.Set(key, value)
		return nil
	}
}

// WithQueryParam sets a single query parameter on the request URL.
func WithQueryParam(key, value string) RequestOption {
	return func(req *http.Request) error {
		q := req.URL.Query()
		q.Set(key, value)
		req.URL.RawQuery = q.Encode()
		return nil
	}
}

// RequestBuilder returns a RequestOption that executes fn against the request.
// Useful when more advanced mutations are needed.
func RequestBuilder(fn func(*http.Request) error) RequestOption {
	return func(req *http.Request) error {
		return fn(req)
	}
}

// Response wraps http.Response with helpers that guarantee cleanup.
type Response struct {
	*http.Response
	closed bool
}

// Close drains and closes the response body to allow connection reuse.
func (r *Response) Close() error {
	if r == nil || r.closed || r.Response == nil || r.Body == nil {
		return nil
	}

	_, _ = io.Copy(io.Discard, r.Body)
	err := r.Body.Close()
	r.closed = true
	return err
}

// ReadAllAndClose reads the entire body and then closes it.
func (r *Response) ReadAllAndClose() ([]byte, error) {
	if r == nil || r.Response == nil || r.Body == nil {
		return nil, nil
	}
	body, err := io.ReadAll(r.Body)
	closeErr := r.Close()
	if err == nil {
		err = closeErr
	}
	return body, err
}

// CopyToAndClose streams the response body to dst and then closes it.
func (r *Response) CopyToAndClose(dst io.Writer) (int64, error) {
	if r == nil || r.Response == nil || r.Body == nil {
		return 0, nil
	}
	n, err := io.Copy(dst, r.Body)
	closeErr := r.Close()
	if err == nil {
		err = closeErr
	}
	return n, err
}

// Do submits the provided request with the supplied client and wraps the response.
func Do(client *http.Client, req *http.Request) (*Response, error) {
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	return &Response{Response: resp}, nil
}

// DoMethod constructs and issues a request using the provided verb.
func DoMethod(ctx context.Context, client *http.Client, method, url string, body io.Reader, opts ...RequestOption) (*Response, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	if strings.Contains(url, "api/") {
		panic("Nodes are not allowed to call external api")
	}

	log.Printf("Making http call to %v\n", url)
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return nil, err
	}

	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt(req); err != nil {
			return nil, err
		}
	}

	return Do(client, req)
}

// Get issues a GET request.
func Get(ctx context.Context, client *http.Client, url string, opts ...RequestOption) (*Response, error) {
	return DoMethod(ctx, client, http.MethodGet, url, nil, opts...)
}

// Head issues a HEAD request.
func Head(ctx context.Context, client *http.Client, url string, opts ...RequestOption) (*Response, error) {
	return DoMethod(ctx, client, http.MethodHead, url, nil, opts...)
}

// Put issues a PUT request with body.
func Put(ctx context.Context, client *http.Client, url string, body io.Reader, opts ...RequestOption) (*Response, error) {
	return DoMethod(ctx, client, http.MethodPut, url, body, opts...)
}

// Post issues a POST request with body.
func Post(ctx context.Context, client *http.Client, url string, body io.Reader, opts ...RequestOption) (*Response, error) {
	return DoMethod(ctx, client, http.MethodPost, url, body, opts...)
}
