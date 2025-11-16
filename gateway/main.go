/*
 * @Author: FunctionSir
 * @License: AGPLv3
 * @Date: 2025-11-15 10:29:22
 * @LastEditTime: 2025-11-16 23:27:08
 * @LastEditors: FunctionSir
 * @Description: Gateway of waifucluster
 * @FilePath: /waifucluster/gateway/main.go
 */

package main

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"image"
	_ "image/jpeg"
	_ "image/png"
	"io"
	"log"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/FunctionSir/readini"
	"github.com/coder/websocket"
	"github.com/google/uuid"
)

var ProcApiListen string
var RegApiListen string
var FrontendDir string
var TempDir string

var WorkersRegToken string

type JobStatus string

const (
	JobStatusWaiting  = "waiting"
	JobStatusPending  = "pending"
	JobStatusFinished = "finished"
	JobStatusError    = "error"
)

type Worker struct {
	Token   string          // Worker token, used as an ID
	conn    *websocket.Conn // WSS conn for worker
	JobChan chan Job
	Name    string // Name of the worker
	Device  string // Device using, like CPU model
}

type Job struct {
	Token      string
	Scale      float64
	Noise      int
	Image      string // Path of image file
	JobWorker  string // Related worker
	Status     JobStatus
	ReceivedAt time.Time
	StartedAt  time.Time
	FinishedAt time.Time
}

type JobToSend struct {
	JobToken string  `json:"job"`
	Scale    float64 `json:"scale"`
	Noise    int     `json:"noise"`
	Image    string  `json:"image"` // Base64RawURLEncoding
	Format   string  `json:"format"`
}

type JobStatusForApi struct {
	Status     JobStatus `json:"status"`
	Device     string    `json:"device"`
	ReceivedAt time.Time `json:"received_at"`
	StartedAt  time.Time `json:"started_at"`
	FinishedAt time.Time `json:"finished_at"`
}

type Result struct {
	JobToken string `json:"job"`
	Ok       bool   `json:"ok"`
	Image    string `json:"image"`
}

var WorkersQueue chan string
var JobsQueue chan string

var Workers sync.Map
var Jobs sync.Map

var TotRegs atomic.Uint64
var TotJobs atomic.Uint64

func (w *Worker) JobSender() {
	for {
		job := <-w.JobChan
		imgData, err := os.ReadFile(job.Image)
		go func(f string) {
			var err error
			if strings.HasPrefix(f, TempDir) {
				err = os.Remove(f)
			}
			if err != nil {
				log.Print("Warning: can not remove " + f)
			}
		}(job.Image)
		if err != nil {
			log.Print("Warning: error occured when sending to worker: ", err)
			job.Status = JobStatusError
			job.FinishedAt = time.Now()
			Jobs.Store(job.Token, job)
			continue
		}
		splited := strings.Split(job.Image, ".")
		toSend := JobToSend{
			JobToken: job.Token,
			Scale:    job.Scale,
			Noise:    job.Noise,
			Image:    base64.RawURLEncoding.EncodeToString(imgData),
			Format:   splited[len(splited)-1],
		}
		jsonData, err := json.Marshal(toSend)
		if err != nil {
			log.Print("Warning: error occured when sending to worker: ", err)
			job.Status = JobStatusError
			job.FinishedAt = time.Now()
			Jobs.Store(job.Token, job)
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
		err = w.conn.Write(ctx, websocket.MessageText, jsonData)
		if err != nil {
			log.Print("Warning: error occured when sending to worker: ", err)
			job.Status = JobStatusError
			job.FinishedAt = time.Now()
			Jobs.Store(job.Token, job)
			cancel()
			continue
		}
		cancel()
		job.Status = JobStatusPending
		job.StartedAt = time.Now()
		job.JobWorker = w.Token
		Jobs.Store(job.Token, job)
	}
}

func (w *Worker) ResultReceiver() {
	for {
		_, data, err := w.conn.Read(context.Background())
		if err != nil {
			Workers.Delete(w.Token)
			break
		}
		res := Result{}
		err = json.Unmarshal(data, &res)
		if err != nil {
			continue
		}
		jobAny, ok := Jobs.Load(res.JobToken)
		if !ok {
			log.Print("A job finished by worker not found.")
			continue
		}
		job := jobAny.(Job)
		if !res.Ok {
			log.Print("Warning: error occured on worker: image: ", job.Image)
			job.Status = JobStatusError
			job.FinishedAt = time.Now()
			Jobs.Store(job.Token, job)
			continue
		}
		imgData, err := base64.RawURLEncoding.DecodeString(res.Image)
		if err != nil {
			log.Print("Warning: error occured when saving result: ", err)
			job.Status = JobStatusError
			job.FinishedAt = time.Now()
			Jobs.Store(job.Token, job)
			continue
		}
		imgPath := path.Join(TempDir, job.Token+".png")
		f, err := os.Create(imgPath)
		if err != nil {
			log.Print("Warning: error occured when saving result: ", err)
			job.Status = JobStatusError
			job.FinishedAt = time.Now()
			Jobs.Store(job.Token, job)
			continue
		}
		_, err = f.Write(imgData)
		if err != nil {
			log.Print("Warning: error occured when saving result: ", err)
			job.Status = JobStatusError
			job.FinishedAt = time.Now()
			Jobs.Store(job.Token, job)
			continue
		}
		job.Image = imgPath
		job.Status = JobStatusFinished
		job.FinishedAt = time.Now()
		Jobs.Store(job.Token, job)
		WorkersQueue <- w.Token
	}
}

func procHandler(w http.ResponseWriter, r *http.Request) {
	receivedAt := time.Now()

	// Limit is 16MiB
	r.Body = http.MaxBytesReader(w, r.Body, 16<<20)

	// Max in mem is 4MiB
	err := r.ParseMultipartForm(4 << 20)
	if err != nil {
		http.Error(w, "malformed request or request entity too large", http.StatusBadRequest)
		return
	}

	scaleStr := r.FormValue("scale")
	scale, err := strconv.ParseFloat(scaleStr, 64)
	if err != nil || scale < 0 || scale > 32 {
		http.Error(w, "invalid scale ratio", http.StatusBadRequest)
		return
	}

	noiseStr := r.FormValue("noise")
	noise, err := strconv.Atoi(noiseStr)
	if err != nil || noise < 0 || noise > 3 {
		http.Error(w, "invalid noise level", http.StatusBadRequest)
		return
	}

	if noise == 0 && scale == 0 {
		http.Error(w, "either noise or scale must be greater than zero", http.StatusBadRequest)
		return
	}

	jobToken := uuid.NewString()

	img, _, err := r.FormFile("image")
	if err != nil {
		http.Error(w, "can not read file properly", http.StatusBadRequest)
		return
	}
	defer func() { _ = img.Close() }()

	imgData, err := io.ReadAll(img)
	if err != nil {
		http.Error(w, "can not read image", http.StatusBadRequest)
		return
	}

	imgInfo, format, err := image.DecodeConfig(bytes.NewReader(imgData))
	if err != nil || (format != "jpeg" && format != "png") {
		http.Error(w, "unsupported image format", http.StatusBadRequest)
		return
	}

	imgPath := path.Join(TempDir, jobToken+"."+format)

	imgFile, err := os.Create(imgPath)
	if err != nil {
		http.Error(w, "can not save the image", http.StatusInternalServerError)
		return
	}
	defer func() { _ = imgFile.Close() }()

	_, err = imgFile.Write(imgData)
	if err != nil {
		http.Error(w, "can not save the image", http.StatusInternalServerError)
		_ = imgFile.Close()
		return
	}
	_ = imgFile.Close()

	newJob := Job{
		Token:      jobToken,
		Scale:      scale,
		Noise:      noise,
		Image:      imgPath,
		Status:     JobStatusWaiting,
		ReceivedAt: receivedAt,
	}

	Jobs.Store(jobToken, newJob)

	JobsQueue <- jobToken

	w.WriteHeader(http.StatusAccepted)
	_, _ = w.Write([]byte(jobToken))

	TotJobs.Add(1)

	log.Print("Received job \"", newJob.Token, "\" ( Size: ", imgInfo.Width, "x", imgInfo.Height, " | Scale: ", newJob.Scale, " | Noise: ", newJob.Noise, " )")
}

func regHandler(w http.ResponseWriter, r *http.Request) {
	token := r.Header.Get("X-Reg-Token")
	if subtle.ConstantTimeCompare([]byte(token), []byte(WorkersRegToken)) != 1 {
		http.Error(w, http.StatusText(http.StatusForbidden), http.StatusForbidden)
		return
	}
	workerToken := r.Header.Get("X-Worker-Token")
	name := r.Header.Get("X-Worker-Name")
	device := r.Header.Get("X-Worker-Device")

	if _, ok := Workers.Load(workerToken); ok {
		http.Error(w, "worker already registered", http.StatusConflict)
		return
	}

	if workerToken == "" || name == "" || device == "" {
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		http.Error(w, "can not reg the worker", http.StatusInternalServerError)
		return
	}
	conn.SetReadLimit(64 << 20)
	newWorker := Worker{
		Token:   workerToken,
		conn:    conn,
		JobChan: make(chan Job),
		Name:    name,
		Device:  device,
	}
	go newWorker.JobSender()
	go newWorker.ResultReceiver()
	Workers.Store(workerToken, newWorker)

	WorkersQueue <- workerToken

	TotRegs.Add(1)

	log.Print("Registered new worker \"", newWorker.Name, "\"")
}

func delivery() {
	for {
		curWorkerToken := <-WorkersQueue
		curWorkerAny, ok := Workers.Load(curWorkerToken)
		if !ok {
			log.Print("Found a worker token in queue, but no related worker found, worker might be offline")
			continue
		}
		curJobToken := <-JobsQueue
		curJobAny, ok := Jobs.Load(curJobToken)
		if !ok {
			log.Print("Found a job token in queue, but no related job found, job might be canceled")
			continue
		}
		curWorker := curWorkerAny.(Worker)
		curJob := curJobAny.(Job)

		curWorker.JobChan <- curJob
	}
}

func statusHandler(w http.ResponseWriter, r *http.Request) {
	jobToken := r.URL.Query().Get("job")
	if jobToken == "" {
		http.Error(w, "job token required", http.StatusBadRequest)
		return
	}
	var jobAny any
	var ok bool
	if jobAny, ok = Jobs.Load(jobToken); !ok {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}
	job := jobAny.(Job)
	device := ""
	if job.Status == JobStatusPending || job.Status == JobStatusFinished {
		workerAny, ok := Workers.Load(job.JobWorker)
		if !ok {
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
		device = workerAny.(Worker).Device
	}
	status := JobStatusForApi{
		Status:     job.Status,
		Device:     device,
		ReceivedAt: job.ReceivedAt,
		StartedAt:  job.StartedAt,
		FinishedAt: job.FinishedAt,
	}
	jsonData, err := json.Marshal(status)
	if err != nil {
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(jsonData)
}

func resultHandler(w http.ResponseWriter, r *http.Request) {
	jobToken := r.URL.Query().Get("job")
	if jobToken == "" {
		http.Error(w, "job token required", http.StatusBadRequest)
		return
	}
	var jobAny any
	var ok bool
	if jobAny, ok = Jobs.Load(jobToken); !ok {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}
	job := jobAny.(Job)
	if job.Status != JobStatusFinished {
		http.Error(w, "job not finished yet", http.StatusAccepted)
		return
	}
	if time.Now().After(job.FinishedAt.Add(7 * 24 * time.Hour)) {
		// Since the file might be cleaned.
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}
	data, err := os.ReadFile(job.Image)
	if err != nil {
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "image/png")
	_, _ = w.Write(data)
}

func deleteHandler(w http.ResponseWriter, r *http.Request) {
	jobToken := r.URL.Query().Get("job")
	if jobToken == "" {
		http.Error(w, "job token required", http.StatusBadRequest)
		return
	}
	var jobAny any
	var ok bool
	if jobAny, ok = Jobs.Load(jobToken); !ok {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}
	Jobs.Delete(jobToken)
	job := jobAny.(Job)
	go func(f string) {
		var err error
		if strings.HasPrefix(f, TempDir) {
			err = os.Remove(f)
		}
		if err != nil {
			log.Print("Warning: can not remove " + f)
			return
		}
	}(job.Image)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("OK"))
}

func cleaner() {
	ticker := time.NewTicker(1 * time.Hour)
	for range ticker.C {
		log.Print("Started to clean outdated files...")
		removed := 0
		now := time.Now()
		Jobs.Range(func(key, val any) bool {
			job := val.(Job)
			if job.Status == JobStatusFinished && now.After(job.FinishedAt.Add((7*24+1)*time.Hour)) { // To prevent conflict.
				Jobs.Delete(key)
				go func(f string) {
					var err error
					if strings.HasPrefix(f, TempDir) {
						err = os.Remove(f)
					}
					if err != nil {
						log.Print("Warning: can not remove " + f)
						return
					}
					removed++
				}(job.Image)
			}
			return true
		})
		log.Print("Removed ", removed, " outdated files")
	}
}

func main() {
	log.Println("Waifu(2x) Cluster Gateway [ Version: 0.1.0 (Furina) ]")
	if len(os.Args) != 2 {
		log.Print("Usage: wcg [conf file]")
		log.Fatal("Args count mismatch.")
	}
	conf, err := readini.LoadFromFile(os.Args[1])
	if err != nil {
		log.Fatal("Can not load config file: ", err)
	}
	ProcApiListen = conf["options"]["ProcListen"]
	RegApiListen = conf["options"]["RegListen"]
	FrontendDir = conf["options"]["FrontendDir"]
	TempDir = conf["options"]["TempDir"]
	WorkersRegToken = conf["options"]["RegToken"]
	if ProcApiListen == "" || RegApiListen == "" ||
		FrontendDir == "" || TempDir == "" || WorkersRegToken == "" {
		log.Fatalln("Config is incomplete")
	}

	WorkersQueue = make(chan string, 256)
	JobsQueue = make(chan string, 65536)

	frontendHandler := http.FileServer(http.Dir(FrontendDir))

	procMux := http.NewServeMux()
	procMux.Handle("/", frontendHandler)
	procMux.HandleFunc("/proc", procHandler)
	procMux.HandleFunc("/status", statusHandler)
	procMux.HandleFunc("/result", resultHandler)
	procMux.HandleFunc("/delete", deleteHandler)

	regMux := http.NewServeMux()
	regMux.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})
	regMux.HandleFunc("/reg", regHandler)

	go func() {
		err := http.ListenAndServe(ProcApiListen, procMux)
		panic(err)
	}()
	go func() {
		err := http.ListenAndServe(RegApiListen, regMux)
		panic(err)
	}()
	go delivery()
	go cleaner()

	for range time.Tick(30 * time.Minute) {
		log.Print("Heartbeat: Total Jobs: ", TotJobs.Load(), " Total Regs: ", TotRegs.Load())
		log.Print("Heartbeat: Waiting Jobs: ", len(JobsQueue), " Relaxing Workers: ", len(WorkersQueue))
	}
}
