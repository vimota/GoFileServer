/* ThreadedIPEchoServer
 */
package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

const msgBufferLength uint32 = 65546

type Request struct {
	cmd     uint8
	msgId   uint32
	length  uint32
	payload []byte
}

type User struct {
	LOGGED_IN bool
}

func main() {
	service := ":1200"
	listener, err := net.Listen("tcp", service)
	checkError(err)
	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		go handleClient(conn)
	}
}

func handleClient(conn net.Conn) {
	defer conn.Close()
	var buf [msgBufferLength]byte
	var bufRes []byte

	user := new(User)
	user.LOGGED_IN = false

	for {
		n, err := conn.Read(buf[0:])
		if err != nil {
			return
		}
		fmt.Fprintf(os.Stderr, "Received: % x\n", buf[0:n])
		request := new(Request)
		parserCommon(request, buf, n)

		fmt.Fprintf(os.Stderr, "cmd: %d\n", request.cmd)
		fmt.Fprintf(os.Stderr, "msgId: %d\n", request.msgId)
		fmt.Fprintf(os.Stderr, "payload: % x\n", request.payload)

		switch request.cmd {
		case 1:
			fmt.Fprintf(os.Stderr, "Login!\n")
			var loginSuccess = login(request)
			if loginSuccess {
				user.LOGGED_IN = true
				fmt.Fprintf(os.Stderr, "Logged in!\n")
				bufRes = respondLogin(request, loginSuccess)
			} else {
				bufRes = respondLogin(request, loginSuccess)
			}
			sendBuffer(conn, bufRes)
		case 2:
			fmt.Fprintf(os.Stderr, "Exit!\n")
			bufRes := wrapCommonHeaders(request, []byte{0x12}, []byte{})
			sendBuffer(conn, bufRes)
			return
		}
	}
}

func sendBuffer(conn net.Conn, bufRes []byte) {
	fmt.Fprintf(os.Stderr, "bufRes: % x\n", bufRes)
	_, err2 := conn.Write(bufRes)
	if err2 != nil {
		return
	}
}

func login(request *Request) bool {
	var usernameLength, passwordLength uint8
	var username, password uint64
	// convert from bytes to ints
	binary.Read(bytes.NewReader(request.payload[0:1]), binary.BigEndian, &usernameLength)
	binary.Read(bytes.NewReader(request.payload[1:5]), binary.BigEndian, &username)
	binary.Read(bytes.NewReader(request.payload[5:6]), binary.BigEndian, &passwordLength)
	binary.Read(bytes.NewReader(request.payload[6:10]), binary.BigEndian, &password)
	fmt.Fprintf(os.Stderr, "username length: %d\n", usernameLength)
	fmt.Fprintf(os.Stderr, "username: %v\n", username)
	fmt.Fprintf(os.Stderr, "password length: %d\n", passwordLength)
	fmt.Fprintf(os.Stderr, "password: %d\n", password)

	if bytes.Equal(request.payload[1:5], request.payload[6:10]) { // username > 1001 && username < 9999 {
		return true
	}
	return false
}

func respondLogin(request *Request, success bool) []byte {
	// make payload
	var payload []byte
	if success {
		payload = []byte{0x01}
	} else {
		payload = []byte{0x00}
	}
	// add common headers
	response := wrapCommonHeaders(request, []byte{0x11}, payload)

	//return
	return response
}

func wrapCommonHeaders(request *Request, cmd []byte, payload []byte) []byte {
	// fmt.Fprintf(os.Stderr, "payload: %v\n", payload)

	// find length of payload to attach to header
	// and convert to bytes
	var length uint32 = uint32(len(payload)) + 5
	lengthBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(lengthBytes, length)

	response := []byte{0x01, 0x11}
	response = append(response, lengthBytes...)
	response = append(response, cmd...)

	msgIdBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(msgIdBytes, request.msgId)
	response = append(response, msgIdBytes...)

	response = append(response, payload...)

	// fmt.Fprintf(os.Stderr, "response: % x\n", response)
	return response
}

func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
}

func parserCommon(request *Request, buf [msgBufferLength]byte, n int) {
	//ProtocolTag	Length			cmd						MsgId			Payload
	//2 Bytes			4 Bytes			1 Byte				4 Bytes		Var (defined by command)
	//0x0111			5 to 65535	Cmd specific	Random		Cmd specific (max 65530)

	binary.Read(bytes.NewReader(buf[2:6]), binary.BigEndian, &request.length)
	binary.Read(bytes.NewReader(buf[6:7]), binary.BigEndian, &request.cmd)
	binary.Read(bytes.NewReader(buf[7:11]), binary.BigEndian, &request.msgId)
	request.payload = buf[11:n]
	return
}
