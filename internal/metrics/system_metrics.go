package metrics

import (
	"context"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

type SystemMetrics struct {
	CPUUsagePct     float64 `json:"cpu_usage_pct"`
	RAMUsedMB       uint64  `json:"ram_used_mb"`
	RAMTotalMB      uint64  `json:"ram_total_mb"`
	GPUAvailable    bool    `json:"gpu_available"`
	GPUEncoderPct   int     `json:"gpu_encoder_pct"`   // NVENC / QSV load %
	GPUMemoryUsedMB int     `json:"gpu_memory_used_mb"`
	GPUTemperatureC int     `json:"gpu_temp_c"`
	GoroutinesCount int     `json:"goroutines_count"`
	UptimeSeconds   int64   `json:"uptime_seconds"`
}

var (
	startTime   = time.Now()
	cpuMu       sync.Mutex
	prevIdle    uint64
	prevTotal   uint64
	hasPrevStat bool
)

// CollectMetrics gathers real CPU, Memory, GPU (nvidia-smi), and runtime metrics
func CollectMetrics() *SystemMetrics {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	ramUsed := m.Alloc / 1024 / 1024
	ramTotal := m.Sys / 1024 / 1024

	// On Linux, read real OS memory if available
	if sysUsed, sysTotal, ok := readLinuxMemInfo(); ok {
		ramUsed = sysUsed
		ramTotal = sysTotal
	}

	gpuAvail, gpuEnc, gpuMem, gpuTemp := queryNvidiaGPU()

	return &SystemMetrics{
		CPUUsagePct:     measureCPULoad(),
		RAMUsedMB:       ramUsed,
		RAMTotalMB:      ramTotal,
		GPUAvailable:    gpuAvail,
		GPUEncoderPct:   gpuEnc,
		GPUMemoryUsedMB: gpuMem,
		GPUTemperatureC: gpuTemp,
		GoroutinesCount: runtime.NumGoroutine(),
		UptimeSeconds:   int64(time.Since(startTime).Seconds()),
	}
}

func queryNvidiaGPU() (available bool, encoderPct int, memUsedMB int, tempC int) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "nvidia-smi", "--query-gpu=utilization.encoder,memory.used,temperature.gpu", "--format=csv,noheader,nounits")
	output, err := cmd.Output()
	if err != nil {
		return false, 0, 0, 0
	}

	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	if len(lines) == 0 {
		return false, 0, 0, 0
	}

	parts := strings.Split(strings.TrimSpace(lines[0]), ",")
	if len(parts) >= 3 {
		encoderPct, _ = strconv.Atoi(strings.TrimSpace(parts[0]))
		memUsedMB, _ = strconv.Atoi(strings.TrimSpace(parts[1]))
		tempC, _ = strconv.Atoi(strings.TrimSpace(parts[2]))
		return true, encoderPct, memUsedMB, tempC
	}

	return false, 0, 0, 0
}

func measureCPULoad() float64 {
	if runtime.GOOS == "linux" {
		if pct, ok := readLinuxProcStatCPU(); ok {
			return pct
		}
	}
	// Cross-platform fallback: Go runtime GC CPU fraction scaled reasonably
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	pct := m.GCCPUFraction * 100.0
	if pct < 0 {
		pct = 0
	}
	if pct > 100 {
		pct = 100
	}
	return pct
}

func readLinuxProcStatCPU() (float64, bool) {
	data, err := os.ReadFile("/proc/stat")
	if err != nil {
		return 0, false
	}
	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		if strings.HasPrefix(line, "cpu ") {
			fields := strings.Fields(line)
			if len(fields) < 5 {
				return 0, false
			}
			var total, idle uint64
			for i := 1; i < len(fields); i++ {
				v, _ := strconv.ParseUint(fields[i], 10, 64)
				total += v
				if i == 4 || i == 5 { // idle + iowait
					idle += v
				}
			}

			cpuMu.Lock()
			defer cpuMu.Unlock()
			if !hasPrevStat {
				prevIdle = idle
				prevTotal = total
				hasPrevStat = true
				return 0, true
			}
			deltaTotal := total - prevTotal
			deltaIdle := idle - prevIdle
			prevIdle = idle
			prevTotal = total
			if deltaTotal == 0 {
				return 0, true
			}
			usage := float64(deltaTotal-deltaIdle) / float64(deltaTotal) * 100.0
			if usage < 0 {
				usage = 0
			}
			if usage > 100 {
				usage = 100
			}
			return usage, true
		}
	}
	return 0, false
}

func readLinuxMemInfo() (usedMB uint64, totalMB uint64, ok bool) {
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0, 0, false
	}
	var memTotalKB, memAvailKB uint64
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "MemTotal:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				memTotalKB, _ = strconv.ParseUint(fields[1], 10, 64)
			}
		} else if strings.HasPrefix(line, "MemAvailable:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				memAvailKB, _ = strconv.ParseUint(fields[1], 10, 64)
			}
		}
	}
	if memTotalKB == 0 {
		return 0, 0, false
	}
	usedKB := uint64(0)
	if memTotalKB > memAvailKB {
		usedKB = memTotalKB - memAvailKB
	}
	return usedKB / 1024, memTotalKB / 1024, true
}
