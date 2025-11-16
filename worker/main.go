package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/FunctionSir/readini"
	"github.com/coder/websocket"
)

var Gateway string
var Token string
var Name string
var RegToken string
var Device string
var TempDir string
var Waifu2xConverterCppBin string
var JobCnt atomic.Uint64
var WorkingTime atomic.Int64

type Job struct {
	JobToken string  `json:"job"`
	Scale    float64 `json:"scale"`
	Noise    int     `json:"noise"`
	Image    string  `json:"image"` // Base64RawURLEncoding
	Format   string  `json:"format"`
}

type Result struct {
	JobToken string `json:"job"`
	Ok       bool   `json:"ok"`
	Image    string `json:"image"`
}

func sendWorkerError(conn *websocket.Conn, job string) {
	data, err := json.Marshal(Result{JobToken: job, Ok: false, Image: ""})
	if err != nil {
		panic(err)
	}
	err = conn.Write(context.Background(), websocket.MessageText, data)
	if err != nil {
		panic(err)
	}
}

func main() {
	log.Println("Waifu(2x) Cluster Worker [ Version: 0.1.0 (Furina) ]")
	if len(os.Args) != 2 {
		log.Print("Usage: wcw [conf file]")
		log.Fatal("Args count mismatch.")
	}
	conf, err := readini.LoadFromFile(os.Args[1])
	if err != nil {
		log.Fatal("Can not load config file: ", err)
	}
	Gateway = conf["options"]["Gateway"]
	RegToken = conf["options"]["RegToken"]
	Device = conf["options"]["Device"]
	TempDir = conf["options"]["TempDir"]
	Name = conf["options"]["Name"]
	Token = conf["options"]["Token"]
	Waifu2xConverterCppBin = conf["options"]["Program"]
	headers := http.Header{}
	headers.Add("X-Reg-Token", RegToken)
	headers.Add("X-Worker-Token", Token)
	headers.Add("X-Worker-Name", Name)
	headers.Add("X-Worker-Device", Device)
	conn, _, err := websocket.Dial(context.Background(), Gateway, &websocket.DialOptions{HTTPHeader: headers})
	if err != nil {
		panic(err)
	}
	conn.SetReadLimit(64 << 20)
	go func() {
		for range time.Tick(30 * time.Minute) {
			log.Println("Heartbeat: Total Jobs Received: ", JobCnt.Load(), " Total Working Time: ", time.Duration(WorkingTime.Load()))
		}
	}()
	for {
		func() {
			_, data, err := conn.Read(context.Background())
			start := time.Now()
			JobCnt.Add(1)
			if err != nil {
				panic(err)
			}
			var job Job
			err = json.Unmarshal(data, &job)
			if err != nil {
				panic(err)
			}
			defer func() {
				since := time.Since(start)
				log.Print("Job ", job.JobToken, " finished, time used: ", since)
				WorkingTime.Add(int64(since))
			}()
			log.Print("Got job ", job.JobToken)
			imgPath := path.Join(TempDir, job.JobToken+"."+job.Format)
			defer func() {
				go func(inImg string) {
					if strings.HasPrefix(inImg, TempDir) {
						_ = os.Remove(inImg)
					}
				}(imgPath)
			}()
			func() {
				imgFile, err := os.Create(imgPath)
				if err != nil {
					sendWorkerError(conn, job.JobToken)
					return
				}
				defer func() { _ = imgFile.Close() }()
				imgData, err := base64.RawURLEncoding.DecodeString(job.Image)
				if err != nil {
					sendWorkerError(conn, job.JobToken)
					return
				}
				_, err = imgFile.Write(imgData)
				if err != nil {
					sendWorkerError(conn, job.JobToken)
					return
				}
			}()
			outFile := path.Join(TempDir, job.JobToken+".out.png")
			defer func() {
				go func(outImg string) {
					if strings.HasPrefix(outImg, TempDir) {
						_ = os.Remove(outImg)
					}
				}(outFile)
			}()
			mode := "noise-scale"
			if job.Scale == 0 {
				mode = "noise"
			}
			if job.Noise == 0 {
				mode = "scale"
			}
			cmd := exec.Command(Waifu2xConverterCppBin, "-i", imgPath, "-o", outFile, "-m", mode, "--scale-ratio", strconv.FormatFloat(job.Scale, 'f', -1, 64), "--noise-level", strconv.Itoa(job.Noise), "-s")
			err = cmd.Run()
			if err != nil {
				sendWorkerError(conn, job.JobToken)
				return
			}
			outImg, err := os.ReadFile(outFile)
			if err != nil {
				sendWorkerError(conn, job.JobToken)
				return
			}
			res := Result{
				JobToken: job.JobToken,
				Ok:       true,
				Image:    base64.RawURLEncoding.EncodeToString(outImg),
			}
			jsonData, err := json.Marshal(res)
			if err != nil {
				sendWorkerError(conn, job.JobToken)
				return
			}
			err = conn.Write(context.Background(), websocket.MessageText, jsonData)
			if err != nil {
				panic(err)
			}
		}()
	}
}
