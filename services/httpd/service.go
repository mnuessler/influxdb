package httpd

import (
	"crypto/tls"
	"expvar"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strings"

	"github.com/influxdb/influxdb"
)

// statistics gathered by the httpd package.
const (
	statRequest                      = "req"                 // Number of HTTP requests served
	statCQRequest                    = "cq_req"              // Number of CQ-execute requests served
	statQueryRequest                 = "query_req"           // Number of query requests served
	statWriteRequest                 = "write_req"           // Number of write requests serverd
	statPingRequest                  = "ping_req"            // Number of ping requests served
	statWriteRequestBytesReceived    = "write_req_bytes"     // Sum of all bytes in write requests
	statQueryRequestBytesTransmitted = "query_resp_bytes"    // Sum of all bytes returned in query reponses
	statPointsWrittenOK              = "points_written_ok"   // Number of points written OK
	statPointsWrittenFail            = "points_written_fail" // Number of points that failed to be written
	statAuthFail                     = "auth_fail"           // Number of authentication failures
)

// Service manages the listener and handler for an HTTP endpoint.
type Service struct {
	ln    net.Listener
	addr  string
	https bool
	cert  string
	err   chan error

	Handler *Handler

	Logger  *log.Logger
	statMap *expvar.Map
}

// NewService returns a new instance of Service.
func NewService(c Config) *Service {
	// Configure expvar monitoring. It's OK to do this even if the service fails to open and
	// should be done before any data could arrive for the service.
	key := strings.Join([]string{"httpd", c.BindAddress}, ":")
	tags := map[string]string{"bind": c.BindAddress}
	statMap := influxdb.NewStatistics(key, "httpd", tags)

	s := &Service{
		addr:  c.BindAddress,
		https: c.HttpsEnabled,
		cert:  c.HttpsCertificate,
		err:   make(chan error),
		Handler: NewHandler(
			c.AuthEnabled,
			c.LogEnabled,
			c.WriteTracing,
			statMap,
		),
		Logger: log.New(os.Stderr, "[httpd] ", log.LstdFlags),
	}
	s.Handler.Logger = s.Logger
	return s
}

// Open starts the service
func (s *Service) Open() error {
	s.Logger.Println("Starting HTTP service")
	s.Logger.Println("Authentication enabled:", s.Handler.requireAuthentication)

	// Open listeners.
	var listeners []net.Listener
	if s.https {
		// Open HTTPS listener if enabled.
		cert, err := tls.LoadX509KeyPair(s.cert, s.cert)
		if err != nil {
			return err
		}

		// TODO: Listener address hardcoded for now, but
		// should be configurable
		httpsListenAddress := ":8087"

		httpsListener, err := tls.Listen("tcp", httpsListenAddress, &tls.Config{
			Certificates: []tls.Certificate{cert},
		})
		if err != nil {
			return err
		}

		s.Logger.Println("Listening on HTTPS:", httpsListener.Addr().String())
		listeners = append(listeners, httpsListener)
	}
	// Always open HTTP listener.
	httpListener, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}

	listeners = append(listeners, httpListener)
	s.Logger.Println("Listening on HTTP:", httpListener.Addr().String())

	// Begin listening for requests in a separate goroutine.
	for _, listener := range listeners {
		go s.serve(listener)
	}
	return nil
}

// Close closes the underlying listener.
func (s *Service) Close() error {
	if s.ln != nil {
		return s.ln.Close()
	}
	return nil
}

// SetLogger sets the internal logger to the logger passed in.
func (s *Service) SetLogger(l *log.Logger) {
	s.Logger = l
}

// Err returns a channel for fatal errors that occur on the listener.
func (s *Service) Err() <-chan error { return s.err }

// Addr returns the listener's address. Returns nil if listener is closed.
func (s *Service) Addr() net.Addr {
	if s.ln != nil {
		return s.ln.Addr()
	}
	return nil
}

// serve serves the handler from the listener.
func (s *Service) serve(listener net.Listener) {
	// The listener was closed so exit
	// See https://github.com/golang/go/issues/4373
	err := http.Serve(listener, s.Handler)
	if err != nil && !strings.Contains(err.Error(), "closed") {
		s.err <- fmt.Errorf("listener failed: addr=%s, err=%s", listener.Addr(), err)
	}
}
