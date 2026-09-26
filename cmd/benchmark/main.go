package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type Config struct {
	Endpoint     string
	AccessKey    string
	SecretKey    string
	Region       string
	Bucket       string
	FileSizeKB   int
	StartUsers   int
	StepUsers    int
	MaxUsers     int
	StepDuration time.Duration
	AutoDetect   bool
}

type StepStats struct {
	Concurrency int
	Duration    time.Duration
	TotalOps    int64
	SuccessOps  int64
	ErrorOps    int64
	BytesTransferred int64
	Latencies   []time.Duration
}

func (s *StepStats) OpsPerSec() float64 {
	if s.Duration <= 0 {
		return 0
	}
	return float64(s.TotalOps) / s.Duration.Seconds()
}

func (s *StepStats) MBPerSec() float64 {
	if s.Duration <= 0 {
		return 0
	}
	return (float64(s.BytesTransferred) / (1024 * 1024)) / s.Duration.Seconds()
}

func (s *StepStats) Mbps() float64 {
	return s.MBPerSec() * 8
}

func (s *StepStats) Percentile(p float64) time.Duration {
	if len(s.Latencies) == 0 {
		return 0
	}
	idx := int(float64(len(s.Latencies)) * (p / 100.0))
	if idx >= len(s.Latencies) {
		idx = len(s.Latencies) - 1
	}
	return s.Latencies[idx]
}

func (s *StepStats) AvgLatency() time.Duration {
	if len(s.Latencies) == 0 {
		return 0
	}
	var total time.Duration
	for _, l := range s.Latencies {
		total += l
	}
	return total / time.Duration(len(s.Latencies))
}

type BenchmarkClient struct {
	cfg        Config
	httpClient *http.Client
	sampleData []byte
}

func NewBenchmarkClient(cfg Config) *BenchmarkClient {
	transport := &http.Transport{
		MaxIdleConns:        2000,
		MaxIdleConnsPerHost: 500,
		MaxConnsPerHost:     0,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  true,
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   30 * time.Second,
	}

	data := make([]byte, cfg.FileSizeKB*1024)
	_, _ = rand.Read(data)

	return &BenchmarkClient{
		cfg:        cfg,
		httpClient: client,
		sampleData: data,
	}
}

func (b *BenchmarkClient) signRequest(req *http.Request) {
	req.Header.Set("Authorization", fmt.Sprintf("AWS %s:benchmarksig", b.cfg.AccessKey))
}

func (b *BenchmarkClient) EnsureBucket(ctx context.Context) error {
	url := fmt.Sprintf("%s/%s", strings.TrimRight(b.cfg.Endpoint, "/"), b.cfg.Bucket)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, nil)
	if err != nil {
		return err
	}
	b.signRequest(req)

	resp, err := b.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusConflict {
		return nil
	}

	body, _ := io.ReadAll(resp.Body)
	return fmt.Errorf("ensure bucket status %d: %s", resp.StatusCode, string(body))
}

func (b *BenchmarkClient) UploadObject(ctx context.Context, key string) (int64, error) {
	url := fmt.Sprintf("%s/%s/%s", strings.TrimRight(b.cfg.Endpoint, "/"), b.cfg.Bucket, key)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(b.sampleData))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	b.signRequest(req)

	resp, err := b.httpClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("upload status %d: %s", resp.StatusCode, string(body))
	}

	return int64(len(b.sampleData)), nil
}

func (b *BenchmarkClient) DownloadObject(ctx context.Context, key string) (int64, error) {
	url := fmt.Sprintf("%s/%s/%s", strings.TrimRight(b.cfg.Endpoint, "/"), b.cfg.Bucket, key)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, err
	}
	b.signRequest(req)

	resp, err := b.httpClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("download status %d: %s", resp.StatusCode, string(body))
	}

	n, err := io.Copy(io.Discard, resp.Body)
	return n, err
}

func (b *BenchmarkClient) DeleteObject(ctx context.Context, key string) error {
	url := fmt.Sprintf("%s/%s/%s", strings.TrimRight(b.cfg.Endpoint, "/"), b.cfg.Bucket, key)
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
	if err != nil {
		return err
	}
	b.signRequest(req)

	resp, err := b.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil
}

func (b *BenchmarkClient) RunStep(ctx context.Context, concurrency int, duration time.Duration) StepStats {
	var totalOps int64
	var successOps int64
	var errorOps int64
	var totalBytes int64

	latenciesMu := sync.Mutex{}
	latencies := make([]time.Duration, 0, concurrency*200)

	stepCtx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()

	var wg sync.WaitGroup
	startTime := time.Now()

	for workerID := 0; workerID < concurrency; workerID++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			seq := 0

			for {
				select {
				case <-stepCtx.Done():
					return
				default:
				}

				key := fmt.Sprintf("bench_%d_%d.bin", id, seq)
				seq++

				// Upload operation
				opStart := time.Now()
				bytesUploaded, err := b.UploadObject(stepCtx, key)
				latency := time.Since(opStart)

				atomic.AddInt64(&totalOps, 1)
				if err != nil {
					if stepCtx.Err() != nil {
						return
					}
					atomic.AddInt64(&errorOps, 1)
				} else {
					atomic.AddInt64(&successOps, 1)
					atomic.AddInt64(&totalBytes, bytesUploaded)

					latenciesMu.Lock()
					latencies = append(latencies, latency)
					latenciesMu.Unlock()
				}

				// Download operation on successful upload
				if err == nil {
					opStart = time.Now()
					bytesDownloaded, errDown := b.DownloadObject(stepCtx, key)
					latencyDown := time.Since(opStart)

					atomic.AddInt64(&totalOps, 1)
					if errDown != nil {
						if stepCtx.Err() != nil {
							return
						}
						atomic.AddInt64(&errorOps, 1)
					} else {
						atomic.AddInt64(&successOps, 1)
						atomic.AddInt64(&totalBytes, bytesDownloaded)

						latenciesMu.Lock()
						latencies = append(latencies, latencyDown)
						latenciesMu.Unlock()
					}

					// Async delete to keep storage clean
					go func(k string) {
						delCtx, c := context.WithTimeout(context.Background(), 5*time.Second)
						defer c()
						_ = b.DeleteObject(delCtx, k)
					}(key)
				}
			}
		}(workerID)
	}

	wg.Wait()
	actualDuration := time.Since(startTime)

	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})

	return StepStats{
		Concurrency:      concurrency,
		Duration:         actualDuration,
		TotalOps:         atomic.LoadInt64(&totalOps),
		SuccessOps:       atomic.LoadInt64(&successOps),
		ErrorOps:         atomic.LoadInt64(&errorOps),
		BytesTransferred: atomic.LoadInt64(&totalBytes),
		Latencies:        latencies,
	}
}

func main() {
	endpointFlag := flag.String("endpoint", "http://localhost:8080", "HF2S3 server endpoint URL")
	accessKeyFlag := flag.String("access-key", "hf2s3-access-key", "S3 Access Key ID")
	secretKeyFlag := flag.String("secret-key", "hf2s3-secret-key", "S3 Secret Access Key")
	regionFlag := flag.String("region", "us-east-1", "S3 Region name")
	bucketFlag := flag.String("bucket", "test", "Bucket name for testing")
	fileSizeFlag := flag.Int("file-size-kb", 256, "Size of test files in Kilobytes")
	startUsersFlag := flag.Int("start-users", 5, "Initial number of concurrent users")
	stepUsersFlag := flag.Int("step", 10, "Increment step for concurrent users")
	maxUsersFlag := flag.Int("max-users", 100, "Maximum number of concurrent users to test")
	durationFlag := flag.Duration("duration", 5*time.Second, "Test duration per concurrency step")
	autoDetectFlag := flag.Bool("auto-detect", true, "Stop automatically when saturation/degradation is detected")
	flag.Parse()

	cfg := Config{
		Endpoint:     *endpointFlag,
		AccessKey:    *accessKeyFlag,
		SecretKey:    *secretKeyFlag,
		Region:       *regionFlag,
		Bucket:       *bucketFlag,
		FileSizeKB:   *fileSizeFlag,
		StartUsers:   *startUsersFlag,
		StepUsers:    *stepUsersFlag,
		MaxUsers:     *maxUsersFlag,
		StepDuration: *durationFlag,
		AutoDetect:   *autoDetectFlag,
	}

	fmt.Println("--- HF2S3: AUTOMATED CONCURRENCY & BANDWIDTH BENCHMARK TOOL ---")
	fmt.Printf(" [Target Endpoint]   %s\n", cfg.Endpoint)
	fmt.Printf(" [Target Bucket]     %s\n", cfg.Bucket)
	fmt.Printf(" [File Size]         %d KB (Upload + Download round-trips)\n", cfg.FileSizeKB)
	fmt.Printf(" [Concurrency Ramp]  %d to %d users (step: +%d users, %v/step)\n", cfg.StartUsers, cfg.MaxUsers, cfg.StepUsers, cfg.StepDuration)
	fmt.Println("==========================================================================================")

	client := NewBenchmarkClient(cfg)

	// Verify server connection & ensure bucket exists
	fmt.Printf("--> Verifying connection to %s and preparing bucket '%s'...\n", cfg.Endpoint, cfg.Bucket)
	initCtx, initCancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := client.EnsureBucket(initCtx); err != nil {
		initCancel()
		fmt.Printf("[FATAL ERROR] Cannot connect to HF2S3 gateway or prepare bucket: %v\n", err)
		fmt.Println("Please make sure 'hf2s3.exe' is running and accessible at http://localhost:8080.")
		os.Exit(1)
	}
	initCancel()
	fmt.Println("--> Bucket confirmed ready! Starting automated concurrency stress tests...")
	fmt.Println()

	// Print table header
	fmt.Printf("%-8s | %-10s | %-12s | %-12s | %-10s | %-10s | %-10s | %-10s\n",
		"USERS", "OPS/SEC", "THROUGHPUT", "LINE SPEED", "AVG LAT", "P95 LAT", "SUCCESS%", "STATUS")
	fmt.Println("---------------------------------------------------------------------------------------------------------")

	// Handle graceful abort with Ctrl+C
	stopSignal := make(chan os.Signal, 1)
	signal.Notify(stopSignal, os.Interrupt, syscall.SIGTERM)
	ctx, cancelAll := context.WithCancel(context.Background())
	defer cancelAll()

	go func() {
		<-stopSignal
		fmt.Println("\n[!] Benchmark interrupted by user. Finalizing report...")
		cancelAll()
	}()

	var allSteps []StepStats
	var peakUsers int
	var peakOps float64
	var peakMbps float64
	var saturationUsers int

	for u := cfg.StartUsers; u <= cfg.MaxUsers; u += cfg.StepUsers {
		select {
		case <-ctx.Done():
			break
		default:
		}

		stats := client.RunStep(ctx, u, cfg.StepDuration)
		allSteps = append(allSteps, stats)

		successRate := float64(0)
		if stats.TotalOps > 0 {
			successRate = (float64(stats.SuccessOps) / float64(stats.TotalOps)) * 100.0
		}

		p95 := stats.Percentile(95)
		status := "OPTIMAL"
		isDegraded := false

		if successRate < 95.0 || p95 > 4*time.Second {
			status = "SATURATED"
			isDegraded = true
			if saturationUsers == 0 {
				saturationUsers = u
			}
		} else if successRate < 99.0 || p95 > 2*time.Second {
			status = "WARNING"
		}

		fmt.Printf("%-8d | %-10.1f | %-8.2f MB/s | %-8.2f Mbps | %-10v | %-10v | %-9.1f%% | %-10s\n",
			stats.Concurrency,
			stats.OpsPerSec(),
			stats.MBPerSec(),
			stats.Mbps(),
			stats.AvgLatency().Round(time.Millisecond),
			p95.Round(time.Millisecond),
			successRate,
			status,
		)

		if stats.OpsPerSec() > peakOps {
			peakOps = stats.OpsPerSec()
			peakUsers = u
		}
		if stats.Mbps() > peakMbps {
			peakMbps = stats.Mbps()
		}

		// Auto-stop if saturation detected for 2 consecutive steps or error rate high
		if cfg.AutoDetect && isDegraded && u > cfg.StartUsers {
			fmt.Printf("\n[ALERT] System reached saturation limit at %d concurrent users.\n", u)
			break
		}
	}

	// Print final analysis report
	fmt.Println("\n==========================================================================================")
	fmt.Println("                       BENCHMARK RESULTS & CAPACITY SUMMARY")
	fmt.Println("==========================================================================================")
	if peakUsers > 0 {
		fmt.Printf(" [Peak Concurrency Tested]     %d concurrent users\n", peakUsers)
		fmt.Printf(" [Max Operations Rate]         %.1f ops/sec (PUT + GET)\n", peakOps)
		fmt.Printf(" [Peak Measured Line Speed]    %.2f Mbps (%.2f MB/s)\n", peakMbps, peakMbps/8)
	}

	if saturationUsers > 0 {
		fmt.Printf(" [Max Stable Concurrency]      ~%d concurrent users (before latency spikes)\n", saturationUsers-cfg.StepUsers)
	} else if len(allSteps) > 0 {
		lastStep := allSteps[len(allSteps)-1]
		fmt.Printf(" [Max Stable Concurrency]      >= %d concurrent users (No degradation observed!)\n", lastStep.Concurrency)
	}

	fmt.Println("==========================================================================================")
	fmt.Println("                       1 Gbps SERVER CAPACITY ANALYSIS")
	fmt.Println("==========================================================================================")
	fmt.Println(" * Enlace de Red Disponible:    1,000 Mbps (125 MB/s)")
	if peakMbps > 0 {
		pct := (peakMbps / 1000.0) * 100.0
		fmt.Printf(" * Utilización de Banda 1Gbps:  %.1f%% del enlace saturado en este test local\n", pct)
	}
	fmt.Println(" * Usuarios Concurrentes según perfil de uso en tu servidor de 1 Gbps:")
	fmt.Println("   - Streaming / Archivos Grandes (10 Mbps c/u):  ~100 usuarios activos simultáneos (1 Gbps al 100%)")
	fmt.Println("   - Cargas Medianas / Fotos / PDFs (2 Mbps c/u):  ~500 usuarios activos simultáneos")
	fmt.Println("   - Backend / Microservicios (JSON, <500 KB):     1,500 – 3,000+ usuarios concurrentes")
	fmt.Println("==========================================================================================")
}
