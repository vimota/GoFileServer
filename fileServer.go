/* ThreadedIPEchoServer
 */
package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net"
	"os"
	"path"
	"runtime"
	"strconv"
	"time"
)

const msgBufferLength uint32 = 65546

// random number seeding for fileId creation
var r = rand.New(rand.NewSource(time.Now().UTC().UnixNano()))

type File struct {
	FileId             uint32
	FileSize           uint64
	RelativePathLength uint16
	RelativePath       string
	AbsolutePath       string
}

var files = make(chan map[string]File, 1)
var filesLoaded = make(chan bool, 1)

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
	filesLoaded <- false
	files <- make(map[string]File)
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
			// LOGIN
			fmt.Fprintf(os.Stderr, "Login!\n")
			var loginSuccess = login(request)
			if loginSuccess {
				user.LOGGED_IN = true
				fmt.Fprintf(os.Stderr, "Logged in!\n")
				loadFiles()
				bufRes = respondLogin(request, loginSuccess)
			} else {
				bufRes = respondLogin(request, loginSuccess)
			}
			sendBuffer(conn, bufRes)
		case 2:
			// EXIT
			fmt.Fprintf(os.Stderr, "Exit!\n")
			storeFiles()
			bufRes = wrapCommonHeaders(request, []byte{0x12}, []byte{})
			sendBuffer(conn, bufRes)
			return
		case 4:
			// GET_FILE_IDS
			fmt.Fprintf(os.Stderr, "GetFileIds\n")
			bufRes = respondGetFileIds(request)
			sendBuffer(conn, bufRes)
		case 6:
			// ALLOCATE_FILE
			fileId, success := allocateFile(request)
			fmt.Fprintf(os.Stderr, "fileId %v success %v\n", fileId, success)
			bufRes = respondAllocateFile(request, fileId, success)
			sendBuffer(conn, bufRes)
		}
	}
}

func storeFiles() {
	f := <-files
	b, err := json.Marshal(f)
	check(err)

	fmt.Fprintf(os.Stderr, "filesStruct: %+v\n", f)
	fmt.Fprintf(os.Stderr, "filesStruct marshalled: %v\n", b)
	err = ioutil.WriteFile("filesDb.json", b, 0777)
	check(err)
	files <- f
}

func loadFiles() {
	// loads files json struct from file
	if <-filesLoaded {
		// if files have already been loaded
		// release lock and return
		filesLoaded <- true
		return
	}

	b, err := ioutil.ReadFile("filesDb.json")
	if os.IsNotExist(err) {
		// if file does not exist create file
		_, err := os.Create("filesDb.json")
		if err != nil {
			panic(err)
		}
		b = []byte{}
	}
	<-files
	var f map[string]File
	json.Unmarshal(b, &f)
	if f == nil {
		f = make(map[string]File)
	}
	fmt.Fprintf(os.Stderr, "filesStruct: %+v\n", f)
	files <- f
	filesLoaded <- true
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

	// convert from bytes to uints
	binary.Read(bytes.NewReader(request.payload[0:1]), binary.BigEndian, &usernameLength)
	binary.Read(bytes.NewReader(request.payload[5:6]), binary.BigEndian, &passwordLength)

	// convert ascii bytes to string to uint
	username, _ = strconv.ParseUint(string(request.payload[1:5]), 10, 64)
	password, _ = strconv.ParseUint(string(request.payload[6:10]), 10, 64)

	// fmt.Fprintf(os.Stderr, "username length: %d\n", usernameLength)
	// fmt.Fprintf(os.Stderr, "username: %v\n", password)
	// fmt.Fprintf(os.Stderr, "password length: %d\n", passwordLength)
	// fmt.Fprintf(os.Stderr, "password: %v\n", password)

	if username >= 1001 && username <= 9999 && username == password {
		return true
	}
	return false
}

func check(e error) {
	if e != nil {
		panic(e)
	}
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

func allocateFile(request *Request) (uint32, bool) {
	var relativePathLength uint16
	var relativePath string
	var fileSize uint64
	var fileId uint32

	// convert from bytes to appropriate types
	binary.Read(bytes.NewReader(request.payload[0:2]), binary.BigEndian, &relativePathLength)
	relativePath = string(request.payload[2 : 2+relativePathLength])
	binary.Read(bytes.NewReader(request.payload[2+relativePathLength:]), binary.BigEndian, &fileSize)

	// fmt.Fprintf(os.Stderr, "relative path length: %d\n", relativePathLength)
	// fmt.Fprintf(os.Stderr, "relativePath: %v\n", relativePath)
	// fmt.Fprintf(os.Stderr, "fileSize: %v\n", fileSize)

	var empty = make([]byte, fileSize)
	var err error
	// create directory in relative path to where program is running
	_, currentPath, _, _ := runtime.Caller(1)
	basepath := path.Join(path.Dir(currentPath), path.Dir(relativePath))
	fmt.Fprintf(os.Stderr, "basepath: %v\n", basepath)
	err = os.MkdirAll(basepath, 0777)
	check(err)

	// write file
	absolutePath := path.Join(basepath, path.Base(relativePath))
	err = ioutil.WriteFile(absolutePath, empty, 0777)
	check(err)

	// create file id
	fileId = r.Uint32()
	file := File{fileId, fileSize, relativePathLength, relativePath, absolutePath}
	fmt.Println(file)

	filesTmp := <-files
	filesTmp[strconv.Itoa(int(fileId))] = file
	fmt.Println(filesTmp)
	files <- filesTmp
	return fileId, true
}

func respondAllocateFile(request *Request, fileId uint32, success bool) []byte {
	// make payload
	var payload []byte
	if success {
		payload = []byte{0x01}
	} else {
		payload = []byte{0x00}
	}
	fileIdBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(fileIdBytes, fileId)
	payload = append(payload, fileIdBytes...)
	// add common headers
	response := wrapCommonHeaders(request, []byte{0x16}, payload)

	//return
	return response
}

func respondGetFileIds(request *Request) []byte {
	// make payload
	var payload []byte

	f := <-files
	// release files data
	// in case something wrong here doesn't
	// block whole program
	files <- f

	payloadBuf := new(bytes.Buffer)
	// numberOfFiles
	err := binary.Write(payloadBuf, binary.BigEndian, uint32(len(f)))
	if err != nil {
		fmt.Println("binary.Write failed:", err)
	}

	// FILEDESCRIPTOR_STRUCT per file
	for _, file := range f {
		err = binary.Write(payloadBuf, binary.BigEndian, file.FileId)
		if err != nil {
			fmt.Println("FileId binary.Write failed:", err)
		}

		err = binary.Write(payloadBuf, binary.BigEndian, file.FileSize)
		if err != nil {
			fmt.Println("FileSize binary.Write failed:", err)
		}

		err = binary.Write(payloadBuf, binary.BigEndian, file.RelativePathLength)
		if err != nil {
			fmt.Println("RelativePathLength binary.Write failed:", err)
		}

		_, err = payloadBuf.WriteString(file.RelativePath)
		if err != nil {
			fmt.Println("RelativePath binary.Write failed:", err)
		}
	}

	// // prepend total length data
	totalLength := make([]byte, 4)
	binary.BigEndian.PutUint32(totalLength, uint32(len(payload)))
	payload = append(totalLength, payloadBuf.Bytes()...)

	// add common headers
	response := wrapCommonHeaders(request, []byte{0x14}, payload)

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
