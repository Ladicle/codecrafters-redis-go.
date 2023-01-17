package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
)

const (
	CMD_ECHO    = "echo"
	CMD_PING    = "ping"
	CMD_SET     = "set"
	CMD_GET     = "get"
	CMD_COMMAND = "command"
)

type DataStore struct {
	data map[string]string
	mu   sync.RWMutex
}

var store = DataStore{
	data: map[string]string{},
	mu:   sync.RWMutex{},
}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	log.Println("Logs from your program will appear here!")
	if err := run(); err != nil {
		log.Fatal(err)
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
			if err := connHandler(c); err != nil {
				log.Printf("ERROR: %v\n", err)
			}
		}(conn)
	}
}

func connHandler(conn net.Conn) error {
	defer conn.Close()
	for {
		buf := make([]byte, 4*1024)
		n, err := conn.Read(buf)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		req := string(buf[:n])
		tokens := strings.Split(strings.TrimSpace(req), "\r\n")
		log.Printf("read: %q, tokens=%+v\n", req, tokens)

		var msg string
		switch cmd := strings.ToLower(tokens[2]); cmd {
		case CMD_PING:
			msg = "+PONG\r\n"
		case CMD_ECHO:
			msg = fmt.Sprintf("$%d\r\n%s\r\n", len(tokens[4]), tokens[4])
		case CMD_COMMAND:
			msg = "+OK\r\n"
		case CMD_SET:
			store.mu.Lock()
			store.data[tokens[4]] = tokens[6]
			store.mu.Unlock()
			msg = "+OK\r\n"
		case CMD_GET:
			store.mu.RLock()
			data := store.data[tokens[4]]
			store.mu.RUnlock()
			msg = fmt.Sprintf("+%s\r\n", data)
		default:
			return fmt.Errorf("unexpected command: %s", cmd)
		}

		log.Printf("write: %q\n", msg)
		if _, err := conn.Write([]byte(msg)); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
}
