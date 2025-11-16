package stubs

var EvolveEngine = "GOLWorker.GOLEngine"
var CheckAlive = "GOLWorker.GOLAlive"
var Save = "GOLWorker.GOLSave"
var Quit = "GOLWorker.GOLQuit"
var Kill = "GOLWorker.GOLKill"
var Pause = "GOLWorker.GOLPause"
var Resume = "GOLWorker.GOLResume"
var DistWorker = "GOLWorker.GOLDistWorker"
var GetWorld = "GOLWorker.GOLGetWorld"

type Params struct {
	Turns       int
	Threads     int
	ImageWidth  int
	ImageHeight int
}

type RunRequest struct {
	Params Params
	Turns  int
	World  [][]byte
}
type RunResponse struct {
	Turns int
	World [][]byte
}

type AliveRequest struct{}
type AliveResponse struct {
	Turns int
	Alive int
}

type SaveRequest struct{}
type SaveResponse struct {
	Turns int
	World [][]byte
}

type QuitRequest struct{}
type QuitResponse struct{}

type KillRequest struct {
	WorkerID int
}
type KillResponse struct{}

type PauseRequest struct{}
type PauseResponse struct {
	Turns int
}

type ResumeRequest struct{}
type ResumeResponse struct {
	Turns int
}

type DistWorkerRequest struct {
	StartY              int
	EndY                int
	Height              int
	Width               int
	DistWorkerHaloWorld [][]byte
	WorkerID            int
}
type DistWorkerResponse struct {
	StartY          int
	EndY            int
	DistWorkerWorld [][]byte
}

type WorldRequest struct{}
type WorldResponse struct {
	Turns int
	World [][]byte
}
