package main

import (
	"flag"
	"log"
	"net"
	"net/rpc"
	"os"
	"strings"
	"sync"
	"time"

	"DistGOLServer/stubs"
)

func GOLLogic(startY, endY, globalH, globalW int, world, next [][]byte) {
	alive := func(b byte) bool { return b == 255 }
	for y := startY; y < endY; y++ {
		for x := 0; x < globalW; x++ {
			// Count toroidal neighbours from the current world (read-only)
			n := 0
			for dy := -1; dy <= 1; dy++ {
				for dx := -1; dx <= 1; dx++ {
					if dy == 0 && dx == 0 {
						continue
					}
					ny := (y + dy + globalH) % globalH
					nx := (x + dx + globalW) % globalW
					if alive(world[ny][nx]) {
						n++
					}
				}
			}
			a := world[y][x] == 255
			if a && (n == 2 || n == 3) {
				next[y][x] = 255
			} else if !a && n == 3 {
				next[y][x] = 255
			} else {
				next[y][x] = 0
			}
		}
	}
}

func CountAlive(world [][]byte) int {
	aliveCells := 0
	for y := range world {
		for x := range world[y] {
			if world[y][x] == 255 {
				aliveCells++ //Count all cells that are living
			}
		}
	}
	return aliveCells
}

// Snapshot
func copyCurrentWorld(world [][]byte) [][]byte {
	if world == nil {
		return nil
	}
	cpWorld := make([][]byte, len(world))
	for y := range cpWorld {
		cpWorld[y] = make([]byte, len(world[y])) // length of world[y] is the width in 'y' row
	}

	for y := range cpWorld {
		for x := range cpWorld[y] {
			cpWorld[y][x] = world[y][x]
		}
	}
	return cpWorld
}

type GOLWorker struct {
	mu       sync.RWMutex
	cond     *sync.Cond
	world    [][]byte
	Height   int
	Width    int
	turn     int
	paused   bool
	quit     bool
	children []*rpc.Client
}

func NewGOLWorker() *GOLWorker {
	w := &GOLWorker{}
	w.cond = sync.NewCond(&w.mu)
	return w
}

func worldLocalServer(globalWorld [][]byte, globalH, globalW, threads int) [][]byte {
	next := make([][]byte, globalH)
	for y := range next {
		next[y] = make([]byte, globalW)
	}

	var wg sync.WaitGroup
	for i := 0; i < threads; i++ {
		startY := i * globalH / threads
		endY := (i + 1) * globalH / threads
		wg.Add(1)
		go func(a, b int) {
			defer wg.Done()
			GOLLogic(a, b, globalH, globalW, globalWorld, next)
		}(startY, endY) // assign startY to a & assign endY to b
	}
	wg.Wait()

	return next
}

func worldDistributedServer(globalWorld [][]byte, globalH, globalW int, children []*rpc.Client) ([][]byte, error) {
	DistWorker := len(children)
	if DistWorker == 0 {
		return worldLocalServer(globalWorld, globalH, globalW, 1), nil
	}

	next := make([][]byte, globalH)
	for y := range next {
		next[y] = make([]byte, globalW)
	}

	type work struct {
		DistWorkerID int
		startY       int
		endY         int
	}

	workSlice := make([]work, DistWorker)
	for i := 0; i < DistWorker; i++ {
		startY := (i) * (globalH / DistWorker)
		endY := (i + 1) * (globalH / DistWorker) // e.g. globalH=100, DistWorker=4, 1st endY would be (0+1)*(100/4)=25
		workSlice[i] = work{i, startY, endY}
	}

	errChan := make(chan error, DistWorker)
	var wg sync.WaitGroup

	for _, w := range workSlice {
		wg.Add(1)
		go func(w work) {
			defer wg.Done()

			client := children[w.DistWorkerID]
			localWorldH := w.endY - w.startY   // This is the height of world piece we are giving to the children server
			localHaloWorldH := localWorldH + 2 // This is the height of world piece with top and bottom halo row

			// distWorld dimension is localHaloWorldH x globalW
			// Which means it is the world piece 2D slice that we are passing to remote
			// with top row and bottom row glued onto it
			distWorld := make([][]byte, localHaloWorldH)
			for y := 0; y < localHaloWorldH; y++ {
				distWorld[y] = make([]byte, globalW)
			}

			for y := range distWorld {
				for x := range distWorld[y] {
					// this equation help to glue the correct globalWorld Y row to distWorld y row
					// e.g. we have 100x100 world, the 1st row of our 1st distWorld
					// should be the row we are gluing on top of our 1st world fragment
					// which is the last row of globalWorld, i.e. row 99 of global world
					Y := (w.startY - 1 + y + globalH) % globalH // e.g. (0-1+1+100)%100=0R99, Y = 99
					distWorld[y][x] = globalWorld[Y][x]
				}
			}

			log.Printf("[BROKER] Sending world piece (Row%d to Row%d) with halo world piece (H=%d x W=%d) to GOL_WORKER %d\n", w.startY, w.endY, localHaloWorldH, globalW, w.DistWorkerID)

			// passing over startY and endY tells worker which global rows this world piece represents
			distWorkerReq := stubs.DistWorkerRequest{
				StartY:              w.startY,
				EndY:                w.endY,
				Height:              globalH,
				Width:               globalW,
				DistWorkerHaloWorld: distWorld,
				WorkerID:            w.DistWorkerID,
			}
			distWorkerRes := stubs.DistWorkerResponse{}

			if err := client.Call(stubs.DistWorker, distWorkerReq, &distWorkerRes); err != nil {
				if strings.Contains(err.Error(), "unexpected EOF") {
					log.Printf("[BROKER] GOL_WORKER %d is disconnected when working on (row%d to row%d) (ERROR: %v)\n", w.DistWorkerID, w.startY, w.endY, err)
				} else {
					log.Printf("[BROKER] ERROR from GOL_WORKER %d for world piece (row%d to row%d) (ERROR: %v)\n", w.DistWorkerID, w.startY, w.endY, err)
				}
				errChan <- err
				return
			}

			// Piece back the world piece together into global next (GOLEngine)
			for y := 0; y < localWorldH; y++ {
				for x := 0; x < globalW; x++ {
					Y := w.startY + y
					next[Y][x] = distWorkerRes.DistWorkerWorld[y][x]
				}
			}

			log.Printf("[BROKER] GOL_WORKER %d finished world piece (row%d to row%d)\n", w.DistWorkerID, w.startY, w.endY)

		}(w)
	}
	wg.Wait()
	close(errChan)
	if err, ok := <-errChan; ok {
		return nil, err
	}
	return next, nil
}

func (w *GOLWorker) GOLEngine(req stubs.RunRequest, res *stubs.RunResponse) error {

	w.mu.Lock()
	w.world = req.World
	w.Height = req.Params.ImageHeight
	w.Width = req.Params.ImageWidth
	w.turn = 0
	w.paused = false
	w.quit = false
	w.mu.Unlock()

	globalH := w.Height
	globalW := w.Width

	childServer := len(w.children) > 0 //if there is children server, childServer is true

	for t := 0; t < req.Turns; t++ {
		w.mu.Lock()
		for w.paused && !w.quit {
			w.cond.Wait()
		}
		if w.quit {
			w.mu.Unlock()
			break
		}
		globalWorld := w.world // Take snapshot of the global world currently in w.world
		w.mu.Unlock()

		if childServer {
			log.Printf("[BROKER] Turn %d: Using %d GOL_WORKER(s)\n", t, len(w.children))
		} else {
			log.Printf("[BROKER] Turn %d: Running locally with %d threads\n", t, req.Params.Threads)
		}

		var next [][]byte // create a placeholder for our next world
		var err error     // receiver any error from worldDistributedServer

		if childServer {
			next, err = worldDistributedServer(globalWorld, globalH, globalW, w.children)
		} else {
			next = worldLocalServer(globalWorld, globalH, globalW, req.Params.Threads)
		}

		if err != nil {
			return err
		}

		// update turn number after finishing
		w.mu.Lock()
		w.world, next = next, w.world // swap for next turn
		w.turn = t + 1
		// (w.world already points at the new world)
		w.mu.Unlock()
	}

	w.mu.RLock()
	res.Turns = req.Turns
	res.World = w.world
	w.mu.RUnlock()
	return nil // If successful return no error
}

func (w *GOLWorker) GOLAlive(_ stubs.AliveRequest, res *stubs.AliveResponse) error {
	w.mu.RLock()
	turn := w.turn
	world := w.world
	w.mu.RUnlock()

	res.Turns = turn
	res.Alive = CountAlive(world)
	return nil
}

func (w *GOLWorker) GOLSave(_ stubs.SaveRequest, res *stubs.SaveResponse) error {
	w.mu.RLock()
	turn := w.turn
	//world := w.world // This only copies the slice headers, not the actual 2D slice
	copyWorld := copyCurrentWorld(w.world)
	w.mu.RUnlock()

	log.Printf("[BROKER] Received Save request\n")

	res.Turns = turn
	res.World = copyWorld
	return nil
}

func (w *GOLWorker) GOLQuit(_ stubs.QuitRequest, _ *stubs.QuitResponse) error {
	w.mu.Lock()
	w.quit = true
	w.cond.Broadcast() // wake engine if waiting
	w.mu.Unlock()

	log.Printf("[BROKER] Received Quit request\n")

	return nil
}

func (w *GOLWorker) GOLKill(req stubs.KillRequest, _ *stubs.KillResponse) error {
	w.mu.Lock()
	w.quit = true
	w.paused = false
	w.cond.Broadcast()
	w.mu.Unlock()

	if len(w.children) == 0 {
		log.Printf("[GOL_WORKER %d] Exiting on Kill\n", req.WorkerID)
		os.Exit(0)
		return nil
	}

	log.Printf("[BROKER] Received Kill request\n")

	for id, child := range w.children {
		if child == nil {
			log.Printf("[BROKER] There are no GOL_Worker left to kill\n")
		}
		go func(id int, c *rpc.Client) {
			killWorkerReq := &stubs.KillRequest{
				WorkerID: id,
			}
			killWorkerRes := &stubs.KillResponse{}
			if err := c.Call(stubs.Kill, killWorkerReq, &killWorkerRes); err != nil {
				if strings.Contains(err.Error(), "unexpected EOF") {
					log.Printf("[BROKER] Kill GOL_Worker %d: Worker has been killed (ERROR: %v)\n", id, err)
				} else {
					log.Printf("[BROKER] Kill GOL_Worker %d: Failed to kill (ERROR: %v)\n", id, err)
				}
			} else {
				log.Printf("[BROKER] Kill GOL_Worker %d: Successfully killed\n", id)
			}
		}(id, child)
	}

	// Exit shortly after, to give more time for logging before exit
	go func() {
		time.Sleep(1 * time.Second)
		os.Exit(0)
	}()

	return nil
}

func (w *GOLWorker) GOLPause(_ stubs.PauseRequest, res *stubs.PauseResponse) error {
	w.mu.Lock()
	w.paused = true
	turn := w.turn
	w.mu.Unlock()

	log.Printf("[BROKER] Received Pause request\n")

	res.Turns = turn
	return nil
}

func (w *GOLWorker) GOLResume(_ stubs.ResumeRequest, res *stubs.ResumeResponse) error {
	w.mu.Lock()
	w.paused = false
	w.cond.Broadcast()
	turn := w.turn
	w.mu.Unlock()

	log.Printf("[BROKER] Received Resume request\n")

	res.Turns = turn
	return nil
}

func (w *GOLWorker) GOLGetWorld(_ stubs.WorldRequest, res *stubs.WorldResponse) error {
	w.mu.RLock()
	turn := w.turn
	copyWorld := copyCurrentWorld(w.world)
	w.mu.RUnlock()

	res.Turns = turn
	res.World = copyWorld
	return nil
}

func (w *GOLWorker) GOLDistWorker(req stubs.DistWorkerRequest, res *stubs.DistWorkerResponse) error {
	localHaloWorld := req.DistWorkerHaloWorld
	localHaloH := len(localHaloWorld)
	localActualH := req.EndY - req.StartY
	globalW := req.Width

	log.Printf("[GOL_WORKER %d] Received world piece (Row%d to Row%d) with halo world piece (H=%d x W=%d)\n", req.WorkerID, req.StartY, req.EndY-1, len(localHaloWorld), globalW)

	if localHaloH != localActualH+2 {
		log.Printf("[GOL_WORKER %p] ERROR: halo world piece Height = %d, expected %d\n", w, localHaloH, localActualH+2)
		return nil
	}

	localNext := make([][]byte, localHaloH)
	for y := range localNext {
		localNext[y] = make([]byte, globalW)
	}

	alive := func(b byte) bool { return b == 255 }

	for y := 1; y <= localActualH; y++ {
		for x := 0; x < globalW; x++ {
			// Count toroidal neighbours from the current world (read-only)
			n := 0
			for dy := -1; dy <= 1; dy++ {
				for dx := -1; dx <= 1; dx++ {
					if dy == 0 && dx == 0 {
						continue
					}
					// only this line is different from our GOL logic above, this is bc we have halos now
					// ny stays within 0 and (localHaloH-1)
					ny := y + dy
					nx := (x + dx + globalW) % globalW
					if alive(localHaloWorld[ny][nx]) {
						n++
					}
				}
			}
			a := localHaloWorld[y][x] == 255
			if a && (n == 2 || n == 3) {
				localNext[y][x] = 255
			} else if !a && n == 3 {
				localNext[y][x] = 255
			} else {
				localNext[y][x] = 0
			}
		}
	}

	// Remove the top and bottom row to get the actual world piece we want
	localActualWorld := make([][]byte, localActualH)
	for y := range localActualWorld {
		localActualWorld[y] = make([]byte, globalW)
	}

	for y := 0; y < localActualH; y++ {
		for x := 0; x < globalW; x++ {
			localActualWorld[y][x] = localNext[y+1][x]
		}
	}

	res.StartY = req.StartY
	res.EndY = req.EndY
	res.DistWorkerWorld = localActualWorld

	log.Printf("[GOL_WORKER %d] Finished world piece (Row%d to Row%d)\n", req.WorkerID, req.StartY, req.EndY-1)

	return nil
}

func connectDistServers(addrs []string) ([]*rpc.Client, error) {
	childClients := make([]*rpc.Client, 0, len(addrs))
	for _, addr := range addrs {
		addr = strings.TrimSpace(addr)
		if addr == "" {
			continue
		}
		c, err := rpc.Dial("tcp", addr)
		if err != nil {
			return nil, err
		}

		log.Printf("[BROKER] Connected GOL_WORKER at %s\n", addr)

		childClients = append(childClients, c)
	}
	return childClients, nil
}

func main() {
	// Allow user to choose port (default "8030")
	pAddr := flag.String("port", "8030", "Port to listen on")
	distServersAddr := flag.String("workers", "", "List of distributed server worker(s) host:port (separated by comma)")

	flag.Parse()

	worker := NewGOLWorker()

	if *distServersAddr != "" {
		addrs := strings.Split(*distServersAddr, ",")
		children, err := connectDistServers(addrs)
		if err != nil {
			log.Fatalf("[BROKER] Error connecting to GOL_Worker servers: %v\n", err)
		}
		worker.children = children
		log.Printf("[BROKER] Connected to %d GOL_WORKER worker(s): %v\n", len(children), addrs)
	}

	if err := rpc.Register(worker); err != nil {
		log.Fatal(err)
	}

	// Listen on specified TCP port
	listener, err := net.Listen("tcp", ":"+*pAddr)
	if err != nil {
		log.Fatal("net.Listen error:", err)
	}

	defer func(listener net.Listener) {
		err := listener.Close()
		if err != nil {
			log.Printf("Error closing listener: %v\n", err)
		}
	}(listener)

	log.Printf("GOL Engine listening on port %s\n", *pAddr)

	// Accept and serve local controller
	rpc.Accept(listener)
}
