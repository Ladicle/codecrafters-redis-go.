package main

import (
	"bytes"
	"fmt"
	"net"
	"os"
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")
	if err := run(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func run() error {
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		return fmt.Errorf("Failed to bind to port 6379")
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			return fmt.Errorf("Error accepting connection: %s", err)
		}
		go func(c net.Conn) {
			defer conn.Close()
			for {
				buf := make([]byte, 4*1024)
				if _, err := conn.Read(buf); err != nil {
					fmt.Println(err)
					return
				}
				fmt.Printf("%s\n", buf)
				if bytes.Contains(buf, []byte("PING")) || bytes.Contains(buf, []byte("ping")) {
					fmt.Println("write: PONG")
					if _, err := conn.Write([]byte("+PONG\r\n")); err != nil {
						return
					}
				} else {
					if _, err := conn.Write([]byte("+OK\r\n")); err != nil {
						return
					}
				}
			}
		}(conn)
	}
}
