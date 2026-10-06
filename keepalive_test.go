/*
   Copyright The containerd Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package ttrpc

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/containerd/ttrpc/internal"
)

func TestClientKeepAliveIdleConnection(t *testing.T) {
	var (
		ctx             = context.Background()
		server          = mustServer(t)(NewServer())
		testImpl        = &testingServer{}
		addr, listener  = newTestListener(t)
		closedCh        = make(chan struct{})
		client, cleanup = newTestClient(t, addr,
			WithClientKeepAlive(20*time.Millisecond, 100*time.Millisecond),
			WithOnClose(func() {
				close(closedCh)
			}),
		)
		tclient = newTestingClient(client)
	)
	defer listener.Close()
	defer cleanup()

	registerTestingService(server, testImpl)
	go server.Serve(ctx, listener)
	defer server.Shutdown(ctx)

	// Wait through several ReadIdleTimeout cycles with no RPC traffic.
	time.Sleep(100 * time.Millisecond)

	select {
	case <-closedCh:
		t.Fatal("client connection closed unexpectedly while server was responsive to PINGs")
	default:
	}

	tp := &internal.TestPayload{Foo: "alive"}
	resp, err := tclient.Test(ctx, tp)
	if err != nil {
		t.Fatalf("expected RPC to succeed after idle keepalive cycles, got: %v", err)
	}
	if resp.Foo != "alivealive" {
		t.Fatalf("unexpected response payload: %q", resp.Foo)
	}
}

func TestClientKeepAliveSlowHandler(t *testing.T) {
	var (
		ctx             = context.Background()
		server          = mustServer(t)(NewServer())
		addr, listener  = newTestListener(t)
		closedCh        = make(chan struct{})
		client, cleanup = newTestClient(t, addr,
			WithClientKeepAlive(25*time.Millisecond, 100*time.Millisecond),
			WithOnClose(func() {
				close(closedCh)
			}),
		)
		tclient = newTestingClient(client)
	)
	defer listener.Close()
	defer cleanup()

	server.Register(serviceName, map[string]Method{
		"Test": func(_ context.Context, unmarshal func(any) error) (any, error) {
			var req internal.TestPayload
			if err := unmarshal(&req); err != nil {
				return nil, err
			}
			if req.Foo == "slow" {
				time.Sleep(150 * time.Millisecond)
			}
			return &internal.TestPayload{Foo: req.Foo + "-done"}, nil
		},
	})

	go server.Serve(ctx, listener)
	defer server.Shutdown(ctx)

	t.Run("within_request_deadline", func(t *testing.T) {
		callCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		defer cancel()

		resp, err := tclient.Test(callCtx, &internal.TestPayload{Foo: "slow"})
		if err != nil {
			t.Fatalf("expected slow RPC within deadline to succeed, got: %v", err)
		}
		if resp.Foo != "slow-done" {
			t.Fatalf("unexpected response: %q", resp.Foo)
		}
	})

	t.Run("exceeds_request_deadline_connection_stays_alive", func(t *testing.T) {
		callCtx, cancel := context.WithTimeout(ctx, 40*time.Millisecond)
		defer cancel()

		_, err := tclient.Test(callCtx, &internal.TestPayload{Foo: "slow"})
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expected context.DeadlineExceeded, got: %v", err)
		}

		// Wait for the slow handler on the server to finish and send its late response,
		// while PING/PONG frames keep the connection alive.
		time.Sleep(150 * time.Millisecond)

		select {
		case <-closedCh:
			t.Fatal("expected client connection to remain open after single RPC timeout")
		default:
		}

		// Subsequent fast RPC on the same connection must succeed.
		resp, err := tclient.Test(ctx, &internal.TestPayload{Foo: "fast"})
		if err != nil {
			t.Fatalf("expected subsequent RPC on same connection to succeed, got: %v", err)
		}
		if resp.Foo != "fast-done" {
			t.Fatalf("unexpected response: %q", resp.Foo)
		}
	})
}

func TestClientKeepAliveUnresponsiveServer(t *testing.T) {
	addr, listener := newTestListener(t)
	defer listener.Close()

	serverDone := make(chan struct{})
	defer close(serverDone)

	pingReceived := make(chan messageHeader, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		ch := newChannel(conn)
		mh, _, err := ch.recv()
		if err == nil {
			pingReceived <- mh
		}
		// Never reply with PONG; simulate a wedged server process.
		<-serverDone
	}()

	closedCh := make(chan struct{})
	client, cleanup := newTestClient(t, addr,
		WithClientKeepAlive(30*time.Millisecond, 40*time.Millisecond),
		WithOnClose(func() {
			close(closedCh)
		}),
	)
	defer cleanup()

	select {
	case mh := <-pingReceived:
		if mh.Type != messageTypePing {
			t.Fatalf("expected messageTypePing, got %v", mh.Type)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for client to send PING frame")
	}

	select {
	case <-closedCh:
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for client to close unresponsive connection")
	}

	var tp internal.TestPayload
	if err := client.Call(context.Background(), serviceName, "Test", &tp, &tp); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed on closed client, got: %v", err)
	}
}

func TestServerKeepAlive(t *testing.T) {
	t.Run("negative_timeout_error", func(t *testing.T) {
		if _, err := NewServer(WithServerKeepAlive(-1*time.Second, time.Second)); err == nil {
			t.Fatal("expected error for negative ReadIdleTimeout")
		}
		if _, err := NewServer(WithServerKeepAlive(time.Second, -1*time.Second)); err == nil {
			t.Fatal("expected error for negative PingTimeout")
		}
	})

	t.Run("responsive_client", func(t *testing.T) {
		var (
			ctx             = context.Background()
			server          = mustServer(t)(NewServer(WithServerKeepAlive(20*time.Millisecond, 100*time.Millisecond)))
			testImpl        = &testingServer{}
			addr, listener  = newTestListener(t)
			client, cleanup = newTestClient(t, addr)
			tclient         = newTestingClient(client)
		)
		defer listener.Close()
		defer cleanup()

		registerTestingService(server, testImpl)
		go server.Serve(ctx, listener)
		defer server.Shutdown(ctx)

		// Let server send multiple PINGs and client reply with PONGs.
		time.Sleep(100 * time.Millisecond)

		resp, err := tclient.Test(ctx, &internal.TestPayload{Foo: "ok"})
		if err != nil {
			t.Fatalf("expected RPC to succeed after server keepalive cycles, got: %v", err)
		}
		if resp.Foo != "okok" {
			t.Fatalf("unexpected response: %q", resp.Foo)
		}
	})

	t.Run("unresponsive_client", func(t *testing.T) {
		var (
			ctx            = context.Background()
			server         = mustServer(t)(NewServer(WithServerKeepAlive(30*time.Millisecond, 40*time.Millisecond)))
			addr, listener = newTestListener(t)
		)
		defer listener.Close()

		registerTestingService(server, &testingServer{})
		go server.Serve(ctx, listener)
		defer server.Shutdown(ctx)

		conn, err := net.Dial("unix", addr)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()

		ch := newChannel(conn)
		// Read the server's PING frame, then do not reply with PONG.
		mh, _, err := ch.recv()
		if err != nil {
			t.Fatalf("expected to receive PING from server, got error: %v", err)
		}
		if mh.Type != messageTypePing {
			t.Fatalf("expected messageTypePing, got %v", mh.Type)
		}

		// Next read should return EOF once server's PingTimeout expires and closes the connection.
		_, _, err = ch.recv()
		if !errors.Is(err, io.EOF) {
			t.Fatalf("expected io.EOF after server PingTimeout, got: %v", err)
		}
	})
}
