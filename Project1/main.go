package main

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/olekukonko/tablewriter"
)

func main() {
	// CLI args
	f, closeFile, err := openProcessingFile(os.Args...)
	if err != nil {
		log.Fatal(err)
	}
	defer closeFile()

	// Load and parse processes
	processes, err := loadProcesses(f)
	if err != nil {
		log.Fatal(err)
	}

	// First-come, first-serve scheduling
	FCFSSchedule(os.Stdout, "First-come, first-serve", processes)

	// Shortest Job First - Preemptive
	SJFSchedule(os.Stdout, "Shortest-job-first", processes)

	// Priority Schedule - Preemptive - ***SJF if Equal Priority***
	SJFPrioritySchedule(os.Stdout, "Priority", processes)

	// Round Robin Schedule
	RRSchedule(os.Stdout, "Round-robin", processes)
}

func openProcessingFile(args ...string) (*os.File, func(), error) {
	if len(args) != 2 {
		return nil, nil, fmt.Errorf("%w: must give a scheduling file to process", ErrInvalidArgs)
	}
	// Read in CSV process CSV file
	f, err := os.Open(args[1])
	if err != nil {
		return nil, nil, fmt.Errorf("%v: error opening scheduling file", err)
	}
	closeFn := func() {
		if err := f.Close(); err != nil {
			log.Fatalf("%v: error closing scheduling file", err)
		}
	}

	return f, closeFn, nil
}

type (
	Process struct {
		ProcessID     int64
		ArrivalTime   int64
		BurstDuration int64
		Priority      int64
	}
	RunTime struct {
		ProcessID  int64
		remainTime int64
		waitTime   int64
	}
	TimeSlice struct {
		PID   int64
		Start int64
		Stop  int64
	}
)

//region Schedulers

// FCFSSchedule outputs a schedule of processes in a GANTT chart and a table of timing given:
// • an output writer
// • a title for the chart
// • a slice of processes
func FCFSSchedule(w io.Writer, title string, processes []Process) {
	var (
		serviceTime     int64
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     int64
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)
	)
	for i := range processes {
		if processes[i].ArrivalTime > 0 {
			waitingTime = serviceTime - processes[i].ArrivalTime
		}
		totalWait += float64(waitingTime)

		start := waitingTime + processes[i].ArrivalTime

		turnaround := processes[i].BurstDuration + waitingTime
		totalTurnaround += float64(turnaround)

		completion := processes[i].BurstDuration + processes[i].ArrivalTime + waitingTime
		lastCompletion = float64(completion)

		schedule[i] = []string{
			fmt.Sprint(processes[i].ProcessID),
			fmt.Sprint(processes[i].Priority),
			fmt.Sprint(processes[i].BurstDuration),
			fmt.Sprint(processes[i].ArrivalTime),
			fmt.Sprint(waitingTime),
			fmt.Sprint(turnaround),
			fmt.Sprint(completion),
		}
		serviceTime += processes[i].BurstDuration

		gantt = append(gantt, TimeSlice{
			PID:   processes[i].ProcessID,
			Start: start,
			Stop:  serviceTime,
		})
	}

	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

// SJFSchedule outputs a schedule of processes in a GANTT chart and a table of timing given:
// • an output writer
// • a title for the chart
// • a slice of processes
func SJFSchedule(w io.Writer, title string, processes []Process) {
	var (
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     int64
		totalTime       int64
		activeProc      int64
		procStart       int64
		shortestAvail   int64
		schedule        = make([][]string, len(processes))
		SJFtracker      = make([]RunTime, len(processes))
		gantt           = make([]TimeSlice, 0)
	)

	//Process list is sorted by arrival time

	//Populate tracker and get total run time
	for i := range processes {
		SJFtracker[i].ProcessID = processes[i].ProcessID
		SJFtracker[i].waitTime = 0
		SJFtracker[i].remainTime = processes[i].BurstDuration
		totalTime += processes[i].BurstDuration
	}

	//Set initial index and counter values
	activeProc = 0
	procStart = 0
	shortestAvail = getShortest(SJFtracker, processes, 0)

	//Set starting active process to the shortest process available at start
	activeProc = shortestAvail

	//Processor Loop
	for t := 0; t <= int(totalTime); t++ {
		for i := range processes {

			//If a shorter process arrives on this clock cycle, switch to it
			if (SJFtracker[i].remainTime < SJFtracker[activeProc].remainTime) && (t == int(processes[i].ArrivalTime)) {
				shortestAvail = int64(i)
			}
			//Increment wait time if process has arrived and is not executing
			if (i != int(activeProc) && i != int(shortestAvail)) && (SJFtracker[i].remainTime > 0) && (t > int(processes[i].ArrivalTime)) {
				SJFtracker[i].waitTime += 1
			}
			//Check if the running process is completed, if so change to the new shortest job
			if (i == int(activeProc)) && (SJFtracker[i].remainTime == 0) {
				shortestAvail = getShortest(SJFtracker, processes, int64(t))
			}
			//Decrement the running process remainTime
			if i == int(activeProc) {
				SJFtracker[i].remainTime -= 1
			}

		}

		if activeProc != shortestAvail || t == int(totalTime) {
			gantt = append(gantt, TimeSlice{
				PID:   processes[activeProc].ProcessID,
				Start: procStart,
				Stop:  int64(t),
			})
			procStart = int64(t)
			activeProc = shortestAvail
		}

	}

	//Tabulate final results
	for i := range processes {

		waitingTime = SJFtracker[i].waitTime
		totalWait += float64(waitingTime)

		turnaround := processes[i].BurstDuration + SJFtracker[i].waitTime
		totalTurnaround += float64(turnaround)

		completion := processes[i].BurstDuration + processes[i].ArrivalTime + waitingTime
		lastCompletion = float64(completion)

		schedule[i] = []string{
			fmt.Sprint(processes[i].ProcessID),
			fmt.Sprint(processes[i].Priority),
			fmt.Sprint(processes[i].BurstDuration),
			fmt.Sprint(processes[i].ArrivalTime),
			fmt.Sprint(waitingTime),
			fmt.Sprint(turnaround),
			fmt.Sprint(completion),
		}
	}

	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

// SJFPriortySchedule outputs a schedule of processes in a GANTT chart and a table of timing given:
// • an output writer
// • a title for the chart
// • a slice of processes
func SJFPrioritySchedule(w io.Writer, title string, processes []Process) {
	var (
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     int64
		totalTime       int64
		activeProc      int64
		procStart       int64
		highestAvail    int64
		schedule        = make([][]string, len(processes))
		SJFtracker      = make([]RunTime, len(processes))
		gantt           = make([]TimeSlice, 0)
	)

	//Process list is sorted by arrival time

	//Populate tracker and get total run time
	for i := range processes {
		SJFtracker[i].ProcessID = processes[i].ProcessID
		SJFtracker[i].waitTime = 0
		SJFtracker[i].remainTime = processes[i].BurstDuration
		totalTime += processes[i].BurstDuration
	}

	//Set initial index and counter values
	activeProc = 0
	procStart = 0
	highestAvail = getShortest(SJFtracker, processes, 0)

	//Set starting active process to the shortest process available at start
	activeProc = highestAvail

	//Processor Loop
	for t := 0; t <= int(totalTime); t++ {
		for i := range processes {

			//If a higher priority process arrives on this clock cycle, switch to it
			if (processes[i].Priority < processes[activeProc].Priority) && (t == int(processes[i].ArrivalTime)) {
				highestAvail = int64(i)
			}
			//If an equal priorty process arrives on this cycle, and it is shorter, switch to it
			if (processes[i].Priority == processes[activeProc].Priority) && (t == int(processes[i].ArrivalTime)) && (SJFtracker[i].remainTime < SJFtracker[activeProc].remainTime) {
				highestAvail = int64(i)
			}
			//Increment wait time if process has arrived and is not executing
			if (i != int(activeProc) && i != int(highestAvail)) && (SJFtracker[i].remainTime > 0) && (t > int(processes[i].ArrivalTime)) {
				SJFtracker[i].waitTime += 1
			}
			//Check if the running process is completed, if so change to the next priority job
			if (i == int(activeProc)) && (SJFtracker[i].remainTime == 0) {
				highestAvail = getHighest(SJFtracker, processes, int64(t))
			}
			//Decrement the running process remainTime
			if i == int(activeProc) {
				SJFtracker[i].remainTime -= 1
			}

		}

		if activeProc != highestAvail || t == int(totalTime) {
			gantt = append(gantt, TimeSlice{
				PID:   processes[activeProc].ProcessID,
				Start: procStart,
				Stop:  int64(t),
			})
			procStart = int64(t)
			activeProc = highestAvail
		}

	}

	//Tabulate final results
	for i := range processes {

		waitingTime = SJFtracker[i].waitTime
		totalWait += float64(waitingTime)

		turnaround := processes[i].BurstDuration + SJFtracker[i].waitTime
		totalTurnaround += float64(turnaround)

		completion := processes[i].BurstDuration + processes[i].ArrivalTime + waitingTime
		lastCompletion = float64(completion)

		schedule[i] = []string{
			fmt.Sprint(processes[i].ProcessID),
			fmt.Sprint(processes[i].Priority),
			fmt.Sprint(processes[i].BurstDuration),
			fmt.Sprint(processes[i].ArrivalTime),
			fmt.Sprint(waitingTime),
			fmt.Sprint(turnaround),
			fmt.Sprint(completion),
		}
	}

	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

// RRSchedule outputs a schedule of processes in a GANTT chart and a table of timing given:
// • an output writer
// • a title for the chart
// • a slice of processes
func RRSchedule(w io.Writer, title string, processes []Process) {
	var (
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     int64
		totalTime       int64
		activeProc      int64
		procStart       int64
		nextAvail       int64
		timeQuantum     int64
		schedule        = make([][]string, len(processes))
		SJFtracker      = make([]RunTime, len(processes))
		gantt           = make([]TimeSlice, 0)
	)

	//Process list is sorted by arrival time

	//Populate tracker and get total run time
	for i := range processes {
		SJFtracker[i].ProcessID = processes[i].ProcessID
		SJFtracker[i].waitTime = 0
		SJFtracker[i].remainTime = processes[i].BurstDuration
		totalTime += processes[i].BurstDuration
	}

	//Set initial index and counter values
	activeProc = 0
	procStart = 0
	nextAvail = 0
	timeQuantum = 4

	//Set starting active process to the shortest process available at start
	activeProc = nextAvail

	//Processor Loop
	for t := 1; t <= int(totalTime); t++ {
		for i := range processes {
			// If process has arrived and is not executing - increment wait time
			if i != int(activeProc) && SJFtracker[i].remainTime > 0 && t > int(processes[i].ArrivalTime) {
				SJFtracker[i].waitTime += 1
				continue
			}

			//Check if the running process is completed -OR- if the current quantum has expired, if so change to the next job
			if i == int(activeProc) {
				SJFtracker[i].remainTime -= 1
				timeQuantum -= 1

				//Check if the running process is completed -OR- if the current quantum has expired, if so change to the next job
				if SJFtracker[i].remainTime == 0 || timeQuantum == 0 {
					nextAvail += 1
				}
			}

		}

		if activeProc != nextAvail || t == int(totalTime) {
			gantt = append(gantt, TimeSlice{
				PID:   processes[activeProc].ProcessID,
				Start: procStart,
				Stop:  int64(t),
			})
			procStart = int64(t)

			timeQuantum = 4

			if nextAvail >= int64(len(processes)) {
				for i := range processes {
					if SJFtracker[i].remainTime > 0 {
						nextAvail = int64(i)
						break
					}
				}
			}

			activeProc = nextAvail
		}

	}

	//Tabulate final results
	for i := range processes {

		waitingTime = SJFtracker[i].waitTime
		totalWait += float64(waitingTime)

		turnaround := processes[i].BurstDuration + SJFtracker[i].waitTime
		totalTurnaround += float64(turnaround)

		completion := processes[i].BurstDuration + processes[i].ArrivalTime + waitingTime
		lastCompletion = float64(completion)

		schedule[i] = []string{
			fmt.Sprint(processes[i].ProcessID),
			fmt.Sprint(processes[i].Priority),
			fmt.Sprint(processes[i].BurstDuration),
			fmt.Sprint(processes[i].ArrivalTime),
			fmt.Sprint(waitingTime),
			fmt.Sprint(turnaround),
			fmt.Sprint(completion),
		}
	}

	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

//endregion

//region calculation helpers

// Returns the index of the shortest job that has an arrival time at or before the specified current time
func getShortest(tracker []RunTime, processes []Process, current int64) (shortest int64) {
	shortest = 0

	for i := range processes {
		if tracker[shortest].remainTime <= 0 {
			shortest += 1
			continue
		}
		if (tracker[i].remainTime < tracker[shortest].remainTime) && (processes[i].ArrivalTime <= current) && (tracker[i].remainTime > 0) {
			shortest = int64(i)
		}
	}

	return
}

// Returns index of the highest priortity and shortest job avaiable at the current clock time
func getHighest(tracker []RunTime, processes []Process, current int64) (highest int64) {
	highest = 0

	for i := range processes {
		if tracker[highest].remainTime <= 0 {
			highest += 1
			continue
		}
		if (processes[i].Priority < processes[highest].Priority) && (processes[i].ArrivalTime <= current) && (tracker[i].remainTime > 0) {
			highest = int64(i)
		}
	}

	return
}

//endregion

//region Output helpers

func outputTitle(w io.Writer, title string) {
	_, _ = fmt.Fprintln(w, strings.Repeat("-", len(title)*2))
	_, _ = fmt.Fprintln(w, strings.Repeat(" ", len(title)/2), title)
	_, _ = fmt.Fprintln(w, strings.Repeat("-", len(title)*2))
}

func outputGantt(w io.Writer, gantt []TimeSlice) {
	_, _ = fmt.Fprintln(w, "Gantt schedule")
	_, _ = fmt.Fprint(w, "|")
	for i := range gantt {
		pid := fmt.Sprint(gantt[i].PID)
		padding := strings.Repeat(" ", (8-len(pid))/2)
		_, _ = fmt.Fprint(w, padding, pid, padding, "|")
	}
	_, _ = fmt.Fprintln(w)
	for i := range gantt {
		_, _ = fmt.Fprint(w, fmt.Sprint(gantt[i].Start), "\t")
		if len(gantt)-1 == i {
			_, _ = fmt.Fprint(w, fmt.Sprint(gantt[i].Stop))
		}
	}
	_, _ = fmt.Fprintf(w, "\n\n")
}

func outputSchedule(w io.Writer, rows [][]string, wait, turnaround, throughput float64) {
	_, _ = fmt.Fprintln(w, "Schedule table")
	table := tablewriter.NewWriter(w)
	table.SetHeader([]string{"ID", "Priority", "Burst", "Arrival", "Wait", "Turnaround", "Exit"})
	table.AppendBulk(rows)
	table.SetFooter([]string{"", "", "", "",
		fmt.Sprintf("Average\n%.2f", wait),
		fmt.Sprintf("Average\n%.2f", turnaround),
		fmt.Sprintf("Throughput\n%.2f/t", throughput)})
	table.Render()
}

//endregion

//region Loading processes.

var ErrInvalidArgs = errors.New("invalid args")

func loadProcesses(r io.Reader) ([]Process, error) {
	rows, err := csv.NewReader(r).ReadAll()
	if err != nil {
		return nil, fmt.Errorf("%w: reading CSV", err)
	}

	processes := make([]Process, len(rows))
	for i := range rows {
		processes[i].ProcessID = mustStrToInt(rows[i][0])
		processes[i].BurstDuration = mustStrToInt(rows[i][1])
		processes[i].ArrivalTime = mustStrToInt(rows[i][2])
		if len(rows[i]) == 4 {
			processes[i].Priority = mustStrToInt(rows[i][3])
		}
	}

	return processes, nil
}

func mustStrToInt(s string) int64 {
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	return i
}

//endregion
