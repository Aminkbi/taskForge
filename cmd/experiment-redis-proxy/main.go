// experiment-redis-proxy is a deliberately small latency-injection boundary
// for the registered co-resident network-path environment. It is not a claim
// that loopback plus delay reproduces a remote cloud Redis service.
package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"time"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:6380", "proxy listen address")
	target := flag.String("target", "127.0.0.1:6379", "Redis target address")
	oneWay := flag.Duration("one-way-latency", 500*time.Microsecond, "delay before forwarding each direction")
	flag.Parse()
	if *oneWay < 0 {
		fatal("one-way latency must be non-negative")
	}
	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		fatal("listen: %v", err)
	}
	defer listener.Close()
	fmt.Printf("redis proxy %s -> %s, one-way delay %s\n", *listen, *target, *oneWay)
	for {
		client, err := listener.Accept()
		if err != nil {
			fatal("accept: %v", err)
		}
		go proxy(client, *target, *oneWay)
	}
}

func proxy(client net.Conn, target string, delay time.Duration) {
	defer client.Close()
	server, err := net.Dial("tcp", target)
	if err != nil {
		return
	}
	defer server.Close()
	done := make(chan struct{}, 2)
	forward := func(destination net.Conn, source net.Conn) {
		buffer := make([]byte, 64<<10)
		for {
			n, readErr := source.Read(buffer)
			if n > 0 {
				if delay > 0 {
					time.Sleep(delay)
				}
				if _, err := destination.Write(buffer[:n]); err != nil {
					break
				}
			}
			if readErr != nil {
				break
			}
		}
		if tcp, ok := destination.(*net.TCPConn); ok {
			_ = tcp.CloseWrite()
		}
		done <- struct{}{}
	}
	go forward(server, client)
	go forward(client, server)
	<-done
	<-done
}

func fatal(format string, args ...any) {
	_, _ = fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
