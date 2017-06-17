package main

import (
	"fmt"
	"net/rpc"
	"errors"
	"net"
	"os"
)
type Args struct {
	A, B int
}
type Quotient struct {
	Q, R int
}

type Arith int

func (t *Arith) Multiply (args *Args, reply *int) error {
	*reply = args.A * args.B
	return nil
}

func (t *Arith) Divide (args *Args, reply *Quotient) error {
	if args.B == 0 {
		return errors.New("divide by zero")
	}
	
	reply.Q = args.A / args.B
	reply.R = args.A % args.B
	
	return nil
}

func (t *Arith) Sum (args *Args, reply *int) error {
	*reply = args.A + args.B
	return nil
}

func (t *Arith) Minus (args *Args, reply *int) error {
	*reply = args.A - args.B
	return nil
}

func main () {
	arith := new(Arith)
	rpc.Register(arith)
	
	tcpAddr, err := net.ResolveTCPAddr("tcp", "localhost:13232")
	checkError("ResolveTCPAddr: ", err)
	
	listener, err := net.ListenTCP("tcp", tcpAddr)
	checkError("ListenTCP: ", err)
	
	for {
		conn, err := listener.Accept()
		checkError("Accept: ", err)
		go rpc.ServeConn(conn)
	}
}

func checkError (str string, err error) {
	if err != nil {
		fmt.Println(str, err)
		os.Exit(1)
	}
}