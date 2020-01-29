package mr

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
)

// Shutdown is an RPC method that shuts down the Master's RPC server.
func (m *Master) Shutdown(_, _ *struct{}) error {
	debug("Shutdown: registration server\n")
	close(m.shutdown)
	m.l.Close() // causes the Accept to fail
	return nil
}

// startRPCServer starts the Master's RPC server. It continues accepting RPC
// calls (Register in particular) for as long as the worker is alive.
func (m *Master) startRPCServer() {
	rpcs := rpc.NewServer()
	rpcs.Register(m)
	os.Remove(m.address) // only needed for "unix"
	l, e := net.Listen("unix", m.address)
	if e != nil {
		log.Fatal("RegstrationServer", m.address, " error: ", e)
	}
	m.l = l

	// now that we are listening on the master address, can fork off
	// accepting connections to another thread.
	go func() {
	loop:
		for {
			select {
			case <-m.shutdown:
				break loop
			default:
			}
			conn, err := m.l.Accept()
			if err == nil {
				go func() {
					rpcs.ServeConn(conn)
					conn.Close()
				}()
			} else {
				debug("RegistrationServer: %v", err)
				break
			}
		}
		debug("RegistrationServer: done\n")
	}()
}

// stopRPCServer stops the master RPC server.
// This must be done through an RPC to avoid race conditions between the RPC
// server thread and the current thread.
func (m *Master) stopRPCServer() {
	var reply ShutdownReply
	ok := call(m.address, "Master.Shutdown", new(struct{}), &reply)
	if ok == false {
		fmt.Printf("Cleanup: RPC %s error\n", m.address)
	}
	debug("cleanupRegistration: done\n")
}
