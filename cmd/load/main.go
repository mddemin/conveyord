// Command load enqueues checksum jobs and waits until the queue is empty.
// It is the phase-12 hammer: point it at a running conveyord, then take a
// pprof while it runs.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	var (
		addr = flag.String("addr", "http://127.0.0.1:8080", "conveyord HTTP base URL")
		n    = flag.Int("n", 500, "jobs to enqueue")
		c    = flag.Int("c", 20, "concurrent enqueueers")
	)
	flag.Parse()
	if *n <= 0 || *c <= 0 {
		fmt.Fprintln(os.Stderr, "-n and -c must be positive")
		os.Exit(1)
	}

	body := `{"type":"checksum","payload":{"x":1}}`
	var ok, fail atomic.Int64
	sem := make(chan struct{}, *c)
	var wg sync.WaitGroup
	start := time.Now()
	for range *n {
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			resp, err := http.Post(*addr+"/jobs", "application/json", strings.NewReader(body))
			if err != nil {
				fail.Add(1)
				return
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode == http.StatusCreated {
				ok.Add(1)
			} else {
				fail.Add(1)
			}
		}()
	}
	wg.Wait()
	enq := time.Since(start)
	fmt.Printf("enqueued %d ok %d fail in %s (%.0f/s)\n", ok.Load(), fail.Load(), enq.Round(time.Millisecond), float64(ok.Load())/enq.Seconds())

	deadline := time.Now().Add(2 * time.Minute)
	for {
		pending, err := busy(*addr, "pending")
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		leased, err := busy(*addr, "leased")
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		if !pending && !leased {
			fmt.Printf("drained in %s\n", time.Since(start).Round(time.Millisecond))
			return
		}
		if time.Now().After(deadline) {
			fmt.Fprintln(os.Stderr, "queue still had work after 2m")
			os.Exit(1)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func busy(addr, state string) (bool, error) {
	resp, err := http.Get(addr + "/jobs?state=" + state + "&limit=1")
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return false, fmt.Errorf("GET /jobs?state=%s: %s", state, resp.Status)
	}
	var jobs []json.RawMessage
	if err := json.NewDecoder(resp.Body).Decode(&jobs); err != nil {
		return false, err
	}
	return len(jobs) > 0, nil
}
