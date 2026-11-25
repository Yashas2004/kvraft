package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Yashas2004/kvraft/kv"
	"github.com/Yashas2004/kvraft/raft"
	"github.com/Yashas2004/kvraft/storage"
)

// ---------- cluster ----------

type node struct {
	server    *kv.Server
	rf        *raft.Raft // to read the term and role
	db        *storage.DB
	persister *raft.Persister
	ln        net.Listener
	killed    bool
}

func (nd *node) kill() {
	if nd.killed {
		return
	}
	nd.killed = true
	nd.ln.Close()
	nd.server.Kill()
	nd.db.Close()
	nd.persister.Close()
}

var maxRaftLog = 100000 // set from -snapshot; 0 disables snapshots

func startCluster(n int, root string) ([]string, []*node) {
	listeners := make([]net.Listener, n)
	addrs := make([]string, n)
	for i := 0; i < n; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			log.Fatal(err)
		}
		listeners[i] = ln
		addrs[i] = ln.Addr().String()
	}

	nodes := make([]*node, n)
	for i := 0; i < n; i++ {
		dir := filepath.Join(root, fmt.Sprintf("node%d", i))
		persister, err := raft.OpenPersister(filepath.Join(dir, "raft"))
		if err != nil {
			log.Fatal(err)
		}
		db, err := storage.OpenDB(filepath.Join(dir, "db"), false)
		if err != nil {
			log.Fatal(err)
		}
		applyCh := make(chan raft.ApplyMsg)
		rf := raft.NewRaft(i, n, raft.NewTCPTransport(addrs), applyCh, persister)
		server := kv.NewServer(rf, db, applyCh, maxRaftLog)

		rpcServer := rpc.NewServer()
		if err := raft.RegisterRPC(rpcServer, rf); err != nil {
			log.Fatal(err)
		}
		if err := kv.RegisterRPC(rpcServer, server); err != nil {
			log.Fatal(err)
		}
		go rpcServer.Accept(listeners[i])

		nodes[i] = &node{server: server, rf: rf, db: db, persister: persister, ln: listeners[i]}
	}
	return addrs, nodes
}

func stopCluster(nodes []*node) {
	for _, nd := range nodes {
		nd.kill()
	}
}

func waitLeader(nodes []*node) int {
	for attempt := 0; attempt < 100; attempt++ {
		for i, nd := range nodes {
			if !nd.killed && nd.server.IsLeader() {
				return i
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	log.Fatal("no leader elected")
	return -1
}

// leaderTerm returns the current term as seen by the current leader.
func leaderTerm(nodes []*node) int {
	leader := waitLeader(nodes)
	term, _ := nodes[leader].rf.GetState()
	return term
}

// ---------- load ----------

type loadResult struct {
	elapsed     time.Duration
	latencies   []time.Duration
	completions []time.Duration // completion time of each op, since start
}

// runLoad drives the cluster with `clients` network clients doing 100-byte Puts.
// during (optional) runs concurrently, e.g. to kill the leader mid-run.
func runLoad(addrs []string, clients int, duration time.Duration, during func(start time.Time)) loadResult {
	start := time.Now()
	deadline := start.Add(duration)
	value := make([]byte, 100)

	var mu sync.Mutex
	var result loadResult
	var wg sync.WaitGroup
	for c := 0; c < clients; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ck := kv.NewNetClerk(addrs)
			defer ck.Close()
			var latencies, completions []time.Duration
			for time.Now().Before(deadline) {
				key := []byte(fmt.Sprintf("key%05d", rand.Intn(10000)))
				opStart := time.Now()
				if err := ck.Put(key, value); err != nil {
					continue
				}
				now := time.Now()
				latencies = append(latencies, now.Sub(opStart))
				completions = append(completions, now.Sub(start))
			}
			mu.Lock()
			result.latencies = append(result.latencies, latencies...)
			result.completions = append(result.completions, completions...)
			mu.Unlock()
		}()
	}
	if during != nil {
		during(start)
	}
	wg.Wait()
	result.elapsed = time.Since(start)
	return result
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	return sorted[int(float64(len(sorted)-1)*p)]
}

// summary returns "median (min–max)" of a set of numbers, formatted by f.
func summary(values []float64, f func(float64) string) (float64, string) {
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	median := sorted[len(sorted)/2]
	if len(sorted) == 1 {
		return median, f(median)
	}
	return median, fmt.Sprintf("%s (%s–%s)", f(median), f(sorted[0]), f(sorted[len(sorted)-1]))
}

func formatOps(v float64) string { return fmt.Sprintf("%.0f", v) }
func formatMs(v float64) string  { return fmt.Sprintf("%.1f ms", v) }

// ---------- failover analysis ----------

const bucket = 100 * time.Millisecond

type failoverResult struct {
	longestGap time.Duration // longest stretch after the kill with zero completed writes
	recovery   time.Duration // kill -> throughput back to 50% of pre-kill average; -1 = never
	xs, ys     []float64     // timeline for the chart: seconds, ops/s
}

func analyzeFailover(completions []time.Duration, killedAt, elapsed time.Duration) failoverResult {
	sort.Slice(completions, func(i, j int) bool { return completions[i] < completions[j] })

	// Longest gap between consecutive completions in the 5 s after the kill,
	// including kill -> first completion and last completion -> end of window.
	var result failoverResult
	windowEnd := killedAt + 5*time.Second
	if elapsed < windowEnd {
		windowEnd = elapsed
	}
	previous := killedAt
	for _, c := range completions {
		if c <= killedAt {
			continue
		}
		if c > windowEnd {
			break
		}
		if c-previous > result.longestGap {
			result.longestGap = c - previous
		}
		previous = c
	}
	if windowEnd-previous > result.longestGap { // silence until the end of the window
		result.longestGap = windowEnd - previous
	}

	// Throughput timeline in 100 ms buckets.
	counts := make([]float64, int(elapsed/bucket)+1)
	for _, c := range completions {
		counts[int(c/bucket)]++
	}
	for i := range counts {
		result.xs = append(result.xs, float64(i)*bucket.Seconds())
		result.ys = append(result.ys, counts[i]/bucket.Seconds())
	}

	// Pre-kill average (skip the first second of warm-up).
	killBucket := int(killedAt / bucket)
	sum, n := 0.0, 0
	for i := int(time.Second / bucket); i < killBucket; i++ {
		sum += counts[i]
		n++
	}
	preKillAverage := sum / float64(n)

	// First bucket fully after the kill that is back to 50% of normal.
	result.recovery = -1 // -1 means never recovered
	for i := killBucket + 1; i < len(counts); i++ {
		if counts[i] >= 0.5*preKillAverage {
			result.recovery = time.Duration(i)*bucket - killedAt
			break
		}
	}
	return result
}

// ---------- SVG charts (no dependencies) ----------

const chartW, chartH, margin = 720.0, 320.0, 60.0

func writeBarChart(path, title, yLabel string, labels []string, values []float64) error {
	maxV := 0.0
	for _, v := range values {
		if v > maxV {
			maxV = v
		}
	}
	var b strings.Builder
	fmt.Fprintf(&b, `<svg xmlns="http://www.w3.org/2000/svg" width="%.0f" height="%.0f" font-family="sans-serif" font-size="13">`, chartW, chartH)
	fmt.Fprintf(&b, `<rect width="100%%" height="100%%" fill="white"/>`)
	fmt.Fprintf(&b, `<text x="%.0f" y="24" text-anchor="middle" font-size="16">%s</text>`, chartW/2, title)
	fmt.Fprintf(&b, `<text x="16" y="%.0f" transform="rotate(-90 16 %.0f)" text-anchor="middle">%s</text>`, chartH/2, chartH/2, yLabel)
	fmt.Fprintf(&b, `<line x1="%.0f" y1="%.0f" x2="%.0f" y2="%.0f" stroke="black"/>`, margin, chartH-margin, chartW-margin, chartH-margin)

	slot := (chartW - 2*margin) / float64(len(values))
	for i, v := range values {
		barH := v / maxV * (chartH - 2*margin)
		x := margin + float64(i)*slot + slot*0.2
		y := chartH - margin - barH
		fmt.Fprintf(&b, `<rect x="%.1f" y="%.1f" width="%.1f" height="%.1f" fill="#4C78A8"/>`, x, y, slot*0.6, barH)
		fmt.Fprintf(&b, `<text x="%.1f" y="%.1f" text-anchor="middle">%.0f</text>`, x+slot*0.3, y-6, v)
		fmt.Fprintf(&b, `<text x="%.1f" y="%.1f" text-anchor="middle">%s</text>`, x+slot*0.3, chartH-margin+20, labels[i])
	}
	b.WriteString(`</svg>`)
	return os.WriteFile(path, []byte(b.String()), 0644)
}

func writeLineChart(path, title, xLabel, yLabel string, xs, ys []float64, markerX float64, markerText string) error {
	maxX, maxY := xs[len(xs)-1], 0.0
	for _, y := range ys {
		if y > maxY {
			maxY = y
		}
	}
	px := func(x float64) float64 { return margin + x/maxX*(chartW-2*margin) }
	py := func(y float64) float64 { return chartH - margin - y/maxY*(chartH-2*margin) }

	var b strings.Builder
	fmt.Fprintf(&b, `<svg xmlns="http://www.w3.org/2000/svg" width="%.0f" height="%.0f" font-family="sans-serif" font-size="13">`, chartW, chartH)
	fmt.Fprintf(&b, `<rect width="100%%" height="100%%" fill="white"/>`)
	fmt.Fprintf(&b, `<text x="%.0f" y="24" text-anchor="middle" font-size="16">%s</text>`, chartW/2, title)
	fmt.Fprintf(&b, `<text x="%.0f" y="%.0f" text-anchor="middle">%s</text>`, chartW/2, chartH-16, xLabel)
	fmt.Fprintf(&b, `<text x="16" y="%.0f" transform="rotate(-90 16 %.0f)" text-anchor="middle">%s</text>`, chartH/2, chartH/2, yLabel)
	fmt.Fprintf(&b, `<line x1="%.0f" y1="%.0f" x2="%.0f" y2="%.0f" stroke="black"/>`, margin, chartH-margin, chartW-margin, chartH-margin)
	fmt.Fprintf(&b, `<line x1="%.0f" y1="%.0f" x2="%.0f" y2="%.0f" stroke="black"/>`, margin, margin, margin, chartH-margin)
	fmt.Fprintf(&b, `<text x="%.0f" y="%.0f" text-anchor="end">%.0f</text>`, margin-6, margin+4, maxY)
	fmt.Fprintf(&b, `<text x="%.0f" y="%.0f" text-anchor="end">0</text>`, margin-6, chartH-margin+4)
	fmt.Fprintf(&b, `<text x="%.0f" y="%.0f" text-anchor="end">%.0f</text>`, chartW-margin, chartH-margin+18, maxX)

	var points strings.Builder
	for i := range xs {
		fmt.Fprintf(&points, "%.1f,%.1f ", px(xs[i]), py(ys[i]))
	}
	fmt.Fprintf(&b, `<polyline fill="none" stroke="#4C78A8" stroke-width="2" points="%s"/>`, points.String())
	fmt.Fprintf(&b, `<line x1="%.1f" y1="%.0f" x2="%.1f" y2="%.0f" stroke="#E45756" stroke-dasharray="5,4"/>`, px(markerX), margin, px(markerX), chartH-margin)
	fmt.Fprintf(&b, `<text x="%.1f" y="%.0f" fill="#E45756">%s</text>`, px(markerX)+6, margin+14, markerText)
	b.WriteString(`</svg>`)
	return os.WriteFile(path, []byte(b.String()), 0644)
}

// ---------- main ----------

func main() {
	nodesFlag := flag.String("nodes", "3,5,7", "cluster sizes to benchmark")
	clients := flag.Int("clients", 64, "concurrent network clients")
	duration := flag.Duration("duration", 5*time.Second, "load duration per run")
	repeat := flag.Int("repeat", 5, "runs per measurement; report median (min–max)")
	outDir := flag.String("out", "docs", "where to write results.md and charts")
	snapshot := flag.Int("snapshot", 100000, "maxRaftLog (entries) for each server; 0 disables snapshots")
	flag.Parse()
	maxRaftLog = *snapshot

	root, err := os.MkdirTemp("", "kvraft-bench")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(root)

	// Warm-up run, not recorded. Lets CPU clocks, the OS file cache and
	// Go's heap settle so the first measured config isn't penalized.
	fmt.Println("warming up...")
	warmAddrs, warmNodes := startCluster(3, filepath.Join(root, "warmup"))
	waitLeader(warmNodes)
	runLoad(warmAddrs, *clients, 2*time.Second, nil)
	stopCluster(warmNodes)

	snapshotText := fmt.Sprintf("snapshot every %d entries", maxRaftLog)
	if maxRaftLog == 0 {
		snapshotText = "snapshots disabled"
	}

	var report strings.Builder
	fmt.Fprintf(&report, "# kvraft benchmark results\n\n")
	fmt.Fprintf(&report, "Generated by `go run ./cmd/bench` on %s. Full stack: TCP clients, Raft with fsync, %s, exactly-once. ", time.Now().Format("2006-01-02"), snapshotText)
	fmt.Fprintf(&report, "Each number is the median of %d runs (min–max in brackets).\n\n", *repeat)
	fmt.Fprintf(&report, "**Caveat:** all nodes and all clients run on one laptop, so they compete for the same CPU and disk. ")
	fmt.Fprintf(&report, "Absolute throughput varies by up to about 3x between invocations (power and thermal state); compare numbers within one run, not across runs.\n\n")

	// ---- Experiment 1: throughput vs cluster size ----
	fmt.Fprintf(&report, "## Throughput vs cluster size (%d clients, 100 B puts, %v per run)\n\n", *clients, *duration)
	fmt.Fprintf(&report, "| Nodes | Tolerates failures | ops/s | mean latency | p99 latency | elections during load |\n|---|---|---|---|---|---|\n")
	var labels []string
	var medians []float64
	run := 0
	for _, field := range strings.Split(*nodesFlag, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(field))
		if err != nil {
			log.Fatal(err)
		}
		var throughputs, means, p99s, elections []float64
		for r := 0; r < *repeat; r++ {
			run++
			addrs, nodes := startCluster(n, filepath.Join(root, fmt.Sprintf("run%d", run)))
			termBefore := leaderTerm(nodes)
			result := runLoad(addrs, *clients, *duration, nil)
			termAfter := leaderTerm(nodes)
			stopCluster(nodes)

			sort.Slice(result.latencies, func(i, j int) bool { return result.latencies[i] < result.latencies[j] })
			throughput := float64(len(result.latencies)) / result.elapsed.Seconds()
			mean := float64(*clients) / throughput * 1000 // Little's Law, in ms
			p99 := float64(percentile(result.latencies, 0.99)) / float64(time.Millisecond)
			termJump := termAfter - termBefore

			throughputs = append(throughputs, throughput)
			means = append(means, mean)
			p99s = append(p99s, p99)
			elections = append(elections, float64(termJump))
			fmt.Printf("%d nodes, run %d: %.0f ops/s, mean %.1f ms, p99 %.1f ms, term +%d\n", n, r+1, throughput, mean, p99, termJump)
		}
		medianThroughput, throughputText := summary(throughputs, formatOps)
		_, meanText := summary(means, formatMs)
		_, p99Text := summary(p99s, formatMs)
		_, electionText := summary(elections, formatOps)
		fmt.Fprintf(&report, "| %d | %d | %s | %s | %s | %s |\n", n, (n-1)/2, throughputText, meanText, p99Text, electionText)
		labels = append(labels, fmt.Sprintf("%d nodes", n))
		medians = append(medians, medianThroughput)
	}

	// ---- Experiment 2: failover under load ----
	failoverDuration := 8 * time.Second
	killAt := 3 * time.Second
	stallWindow := 5 * time.Second // matches the window in analyzeFailover
	var gaps, recoveries []float64
	stuck, slow := 0, 0
	var chartRun failoverResult
	var chartKilledAt time.Duration
	for r := 0; r < *repeat; r++ {
		run++
		addrs, nodes := startCluster(3, filepath.Join(root, fmt.Sprintf("run%d", run)))
		waitLeader(nodes)
		var killedAt time.Duration
		result := runLoad(addrs, *clients, failoverDuration, func(start time.Time) {
			time.Sleep(killAt)
			leader := waitLeader(nodes)
			killedAt = time.Since(start)
			nodes[leader].kill()
		})

		analysis := analyzeFailover(result.completions, killedAt, result.elapsed)
		gaps = append(gaps, float64(analysis.longestGap)/float64(time.Millisecond))
		isStuck := analysis.longestGap >= stallWindow-100*time.Millisecond // no write for the whole window

		recoveryText := "not within run"
		if analysis.recovery >= 0 {
			recoveries = append(recoveries, float64(analysis.recovery)/float64(time.Millisecond))
			recoveryText = analysis.recovery.Round(time.Millisecond).String()
		} else if !isStuck {
			slow++
		}
		if isStuck {
			stuck++
			recoveryText = "STUCK"
		}
		fmt.Printf("failover run %d: longest write gap %v, back to 50%% throughput after %s\n",
			r+1, analysis.longestGap.Round(time.Millisecond), recoveryText)

		// Stuck run: dump the survivors' internals twice, 1 s apart, to see which counters are frozen.
		if isStuck {
			for pass := 0; pass < 2; pass++ {
				for i, nd := range nodes {
					if !nd.killed {
						fmt.Printf("  node %d: %s\n", i, nd.rf.DebugState())
					}
				}
				if pass == 0 {
					time.Sleep(time.Second)
					fmt.Println("  one second later:")
				}
			}
		}
		stopCluster(nodes)

		if r == *repeat-1 {
			chartRun = analysis
			chartKilledAt = killedAt
		}
	}
	_, gapText := summary(gaps, formatMs)
	recoveryText := "n/a"
	if len(recoveries) > 0 {
		_, recoveryText = summary(recoveries, formatMs)
	}

	fmt.Fprintf(&report, "\n## Failover under load (3 nodes, %d clients, leader killed at t=3 s)\n\n", *clients)
	fmt.Fprintf(&report, "| Metric | Result |\n|---|---|\n")
	fmt.Fprintf(&report, "| Longest stretch with zero completed writes (5 s window) | %s |\n", gapText)
	fmt.Fprintf(&report, "| Time until throughput is back to 50%% of normal | %s (100 ms resolution, runs that got there) |\n", recoveryText)
	fmt.Fprintf(&report, "| Runs where writes resumed but stayed below 50%% until the end | %d of %d |\n", slow, *repeat)
	fmt.Fprintf(&report, "| Runs stuck (no write for the whole 5 s window) | %d of %d |\n", stuck, *repeat)
	fmt.Fprintf(&report, "\n![throughput vs cluster size](throughput.svg)\n\n![failover timeline](failover.svg)\n")
	if err := os.MkdirAll(*outDir, 0755); err != nil {
		log.Fatal(err)
	}
	if err := writeBarChart(filepath.Join(*outDir, "throughput.svg"), "Throughput vs cluster size (median)", "ops/s", labels, medians); err != nil {
		log.Fatal(err)
	}
	if err := writeLineChart(filepath.Join(*outDir, "failover.svg"), "Write throughput while killing the leader", "seconds", "ops/s",
		chartRun.xs, chartRun.ys, chartKilledAt.Seconds(), "leader killed"); err != nil {
		log.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(*outDir, "results.md"), []byte(report.String()), 0644); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("wrote %s\n", filepath.Join(*outDir, "results.md"))
}
