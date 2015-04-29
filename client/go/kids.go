package kids

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
)

const (
	crSign        byte = 13 // '\r'
	lfSign        byte = 10 // '\n'
	bulkSign      byte = 36 // '$'
	intSign       byte = 58 // ':'
	errSign       byte = 45 // '-'
	okSign        byte = 43 // '+'
	multiBulkSign byte = 42 // '*'
)

type Log struct {
	Pattern string
	Topic   string
	Data    []byte
}

type response struct {
	err  error
	argv [][]byte
}

type Kids struct {
	Logs <-chan *Log

	// TCP连接
	c net.Conn

	// 读写Buffer
	r *bufio.Reader
	w *bufio.Writer

	// resp?
	resp <-chan *response

	//
	stopped <-chan bool
}

func Dial(nettype, addr string) (k *Kids, err error) {
	k = new(Kids)

	// TCP等参数是否需要配置，例如: tcp_nodelay等
	if k.c, err = net.Dial(nettype, addr); err != nil {
		return nil, err
	}
	k.r = bufio.NewReader(k.c)
	k.w = bufio.NewWriter(k.c)

	var stopped = make(chan bool, 2)
	k.stopped = stopped

	// Logs和Resp是什么东西?
	var out = make(chan *Log, 512)
	var resp = make(chan *response)
	k.Logs = out
	k.resp = resp

	go recvResponseRoutine(k.r, resp, out, stopped)

	return k, nil
}

//
// 功能的触发入口
//
func (k *Kids) Log(topic string, data []byte) error {

	var argv = [][]byte{[]byte("LOG"), []byte(topic), data}
	// 将数据发送出去?
	if err := sendRequest(k.w, argv); err != nil {
		return err
	}

	// 读取发送结果
	var resp = <-k.resp
	if resp.err != nil {
		return resp.err
	}

	// 检查返回的结果
	if resp.argv[0][0] == errSign {
		return errors.New(string(resp.argv[0]))
	}

	return nil
}

func (k *Kids) Close() {
	k.c.Close() // close conn (to stop recvResponseRoutine)
	<-k.stopped // recvResponseRoutine stop
}

func (k *Kids) Subscribe(topic string) ([][]byte, error) {
	var argv = [][]byte{[]byte("SUBSCRIBE"), []byte(topic)}
	if err := sendRequest(k.w, argv); err != nil {
		return nil, err
	}

	var resp = <-k.resp
	if resp.err != nil {
		return nil, resp.err
	}

	return resp.argv, nil
}

func (k *Kids) Psubscribe(pattern string) ([][]byte, error) {
	// 订阅消息
	var argv = [][]byte{[]byte("PSUBSCRIBE"), []byte(pattern)}
	if err := sendRequest(k.w, argv); err != nil {
		return nil, err
	}

	// 等待消息
	var resp = <-k.resp
	if resp.err != nil {
		return nil, resp.err
	}

	return resp.argv, nil
}

func (k *Kids) Unsubscribe(topic string) ([][]byte, error) {
	var argv = [][]byte{[]byte("UNSUBSCRIBE"), []byte(topic)}
	if err := sendRequest(k.w, argv); err != nil {
		return nil, err
	}

	var resp = <-k.resp
	if resp.err != nil {
		return nil, resp.err
	}

	return resp.argv, nil
}

func (k *Kids) Punsubscribe(pattern string) ([][]byte, error) {
	var argv = [][]byte{[]byte("PUNSUBSCRIBE"), []byte(pattern)}
	if err := sendRequest(k.w, argv); err != nil {
		return nil, err
	}

	var resp = <-k.resp
	if resp.err != nil {
		return nil, resp.err
	}

	return resp.argv, nil
}

func sendRequest(w *bufio.Writer, argv [][]byte) error {
	var buf bytes.Buffer

	// 1. 类似Redis的协议
	fmt.Fprintf(&buf, "*%d\r\n", len(argv))

	// 长度，数据
	for _, arg := range argv {
		fmt.Fprintf(&buf, "$%d\r\n", len(arg))
		buf.Write(arg)
		buf.Write([]byte("\r\n"))
	}

	// 2. 将数据写出去
	if _, err := w.Write(buf.Bytes()); err != nil {
		return err
	}

	w.Flush()

	return nil
}

/**
单独的线程:

1.  r: 来自远端服务器的反馈?
2.  respOut
    out

3. 出现异常错误, client退出？

*/
func recvResponseRoutine(r *bufio.Reader, respOut chan<- *response, out chan<- *Log, stopped chan<- bool) {
	defer func() { stopped <- true }()

	for {
		// 从READER中读取数据(client --> log)
		resp, err := recvResponse(r)

		// 现在数据有两个去处?
		//
		select {
		case respOut <- &response{err, resp}: // if response needed by command

		default:
			if err != nil {
				return
			}
			// resp要么是单个?
			// 要么是Bulk
			cmd := strings.ToUpper(string(resp[0]))
			switch cmd {
			// PATTERN, TOPIC, Data
			case "MESSAGE":
				out <- &Log{"", string(resp[1]), []byte(resp[2])}
			case "PMESSAGE":
				out <- &Log{string(resp[1]), string(resp[2]), []byte(resp[3])}
			default:
				// drop response
			}
		}
	}
}

//
// 从 r 中读取一个简单对象: intSign, bulkSign
//
func readArgv(r *bufio.Reader) (arg []byte, err error) {
	var s []byte
	if s, err = r.ReadBytes(lfSign); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return nil, err
	}

	if s[0] == bulkSign {
		// read len
		var arglen, readlen int = 0, 0
		//
		if arglen, err = strconv.Atoi(string(s[1 : len(s)-2])); err != nil {
			return nil, err
		}

		// 把所有的数据，包括 \r\n读取完毕
		arg = make([]byte, arglen+2) // make sure to read "\r\n"
		if _, err := io.ReadFull(r, arg[readlen:]); err != nil {
			return nil, err
		}
		return arg[:len(arg)-2], nil

	} else if s[0] == intSign {
		return s[1 : len(s)-2], nil
	}

	return nil, errors.New("Protocol Error: expected $ or : but got " + string(s))
}

//
// 数据类型:
// 1. intSign
// 2. okSign
// 3. multiBulkSign
// 4. bulkSign
//
func recvResponse(r *bufio.Reader) (resp [][]byte, err error) {
	var s, arg []byte
	var argc int
	//
	// 处理原则: 读取失败，则返回 nil, err
	// 中断connection
	//
	if s, err = r.ReadBytes(lfSign); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return nil, err
	}

	switch s[0] {
	case intSign:
		// intSign + Data + "\r\n"
		resp = append(resp, s[:len(s)-2])

	case okSign, errSign:
		// okSign + Data + "\r\n"
		resp = append(resp, s[:len(s)-2])

	case multiBulkSign:
		// read argc
		if argc, err = strconv.Atoi(string(s[1 : len(s)-2])); err != nil {
			return nil, err
		}
		// 读取所有的数据
		// read argv
		for i := 0; i < argc; i++ {
			if arg, err = readArgv(r); err != nil {
				return nil, err
			}
			resp = append(resp, arg)
		}
	default:
		fmt.Printf("%s", s)
		return nil, fmt.Errorf("Protocol Error: unexpected '%c'", s[0])
	}

	return resp, nil
}
