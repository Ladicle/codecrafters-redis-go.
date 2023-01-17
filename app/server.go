package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	CmdEcho    = "echo"
	CmdPing    = "ping"
	CmdSet     = "set"
	CmdGet     = "get"
	CmdCommand = "command"
)

const (
	MsgOk       = "+OK\r\n"
	MsgBulkNull = "$-1\r\n"
)

type Data struct {
	val string
	exp *time.Time
}

type DataStore struct {
	data map[string]Data
	mu   sync.RWMutex
}

var store = DataStore{
	data: map[string]Data{},
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
		case CmdPing:
			msg = "+PONG\r\n"
		case CmdEcho:
			msg = fmt.Sprintf("$%d\r\n%s\r\n", len(tokens[4]), tokens[4])
		case CmdCommand:
			msg = MsgOk
		case CmdSet:
			key := tokens[4]
			data := Data{val: tokens[6]}
			if len(tokens) > 8 && strings.ToLower(tokens[8]) == "px" {
				psms, err := strconv.Atoi(tokens[10])
				if err != nil {
					return err
				}
				exp := time.Now().Add(time.Duration(psms) * time.Millisecond)
				data.exp = &exp
				log.Printf("data expired at %v", exp)
			}
			store.mu.Lock()
			store.data[key] = data
			store.mu.Unlock()
			msg = MsgOk
		case CmdGet:
			store.mu.RLock()
			data, ok := store.data[tokens[4]]
			store.mu.RUnlock()
			if !ok {
				return fmt.Errorf("%s is unknown key", tokens[4])
			}
			if data.exp != nil && data.exp.Before(time.Now()) {
				msg = MsgBulkNull
				log.Printf("data is already expired at %v", data.exp)
			} else {
				msg = fmt.Sprintf("+%s\r\n", data.val)
			}
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
