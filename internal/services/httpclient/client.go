package httpclient

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

var (
	once     sync.Once
	instance *http.Client
)

func Client() *http.Client {
	once.Do(func() {
		transport := &http.Transport{
			TLSClientConfig:     &tls.Config{InsecureSkipVerify: true},
			MaxIdleConns:        50,
			MaxIdleConnsPerHost: 20,
			IdleConnTimeout:     30 * time.Second,
		}
		instance = &http.Client{
			Transport: transport,
			Timeout:   30 * time.Second,
		}
	})
	return instance
}

// Request is a convenience wrapper for making HTTP requests.
type Request struct {
	Method  string
	URL     string
	Headers map[string]string
	Params  map[string]string
	Body    io.Reader
	Timeout time.Duration
}

// Do executes the request and returns the response.
func Do(r Request) (*http.Response, error) {
	if r.Method == "" {
		r.Method = http.MethodGet
	}

	req, err := http.NewRequest(r.Method, r.URL, r.Body)
	if err != nil {
		return nil, err
	}

	for k, v := range r.Headers {
		req.Header.Set(k, v)
	}

	if len(r.Params) > 0 {
		q := req.URL.Query()
		for k, v := range r.Params {
			q.Set(k, v)
		}
		req.URL.RawQuery = q.Encode()
	}

	client := Client()
	if r.Timeout > 0 {
		client = &http.Client{
			Transport: client.Transport,
			Timeout:   r.Timeout,
		}
	}

	return client.Do(req)
}

// GetJSON performs a GET and decodes the response JSON into dst.
func GetJSON(url string, headers map[string]string, params map[string]string, dst any) error {
	resp, err := Do(Request{
		Method:  http.MethodGet,
		URL:     url,
		Headers: headers,
		Params:  params,
	})
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}

	return json.NewDecoder(resp.Body).Decode(dst)
}

// PostJSON performs a POST with a JSON body and decodes the response.
func PostJSON(url string, headers map[string]string, body any, dst any) error {
	var bodyReader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		bodyReader = strings.NewReader(string(b))
	}

	if headers == nil {
		headers = make(map[string]string)
	}
	headers["Content-Type"] = "application/json"

	resp, err := Do(Request{
		Method:  http.MethodPost,
		URL:     url,
		Headers: headers,
		Body:    bodyReader,
	})
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(b))
	}

	if dst != nil {
		return json.NewDecoder(resp.Body).Decode(dst)
	}
	return nil
}
