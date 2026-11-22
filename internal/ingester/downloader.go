package ingester

import (
	"archive/zip"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
)

var ffmpegEnsureMu sync.Mutex

// EnsureFFmpeg checks if ffmpeg is in PATH or ./bin/ffmpeg(.exe). If missing, automatically downloads it for the current OS.
func EnsureFFmpeg() string {
	ffmpegEnsureMu.Lock()
	defer ffmpegEnsureMu.Unlock()

	if path, err := exec.LookPath("ffmpeg"); err == nil {
		return path
	}

	localBin := filepath.Join("bin", "ffmpeg")
	if runtime.GOOS == "windows" {
		localBin = filepath.Join("bin", "ffmpeg.exe")
	}

	if _, err := os.Stat(localBin); err == nil {
		return localBin
	}

	var downloadURL string
	switch runtime.GOOS {
	case "windows":
		downloadURL = "https://github.com/GyanD/codexffmpeg/releases/download/6.0/ffmpeg-6.0-essentials_build.zip"
	case "linux":
		if runtime.GOARCH == "amd64" {
			downloadURL = "https://github.com/ffbinaries/ffbinaries-prebuilt/releases/download/v6.1/ffmpeg-6.1-linux-64.zip"
		} else {
			downloadURL = "https://github.com/ffbinaries/ffbinaries-prebuilt/releases/download/v6.1/ffmpeg-6.1-linux-armhf-32.zip"
		}
	case "darwin":
		downloadURL = "https://github.com/ffbinaries/ffbinaries-prebuilt/releases/download/v6.1/ffmpeg-6.1-macos-64.zip"
	default:
		log.Printf("[FFMPEG-AUTO-INSTALL] Unsupported OS %s for auto-download. Please install ffmpeg in PATH.", runtime.GOOS)
		return "ffmpeg"
	}

	log.Printf("[FFMPEG-AUTO-INSTALL] FFmpeg not found for %s/%s! Downloading standalone binary...", runtime.GOOS, runtime.GOARCH)

	if err := os.MkdirAll("bin", 0755); err != nil {
		log.Printf("Failed to create bin folder: %v", err)
		return "ffmpeg"
	}

	zipPath := filepath.Join("bin", "ffmpeg.zip")
	if err := downloadFile(zipPath, downloadURL); err != nil {
		log.Printf("[FFMPEG-AUTO-INSTALL] Download failed: %v. Please install FFmpeg manually in PATH.", err)
		return "ffmpeg"
	}

	if err := extractFFmpegFromZip(zipPath, localBin); err != nil {
		log.Printf("[FFMPEG-AUTO-INSTALL] Extraction failed: %v", err)
		_ = os.Remove(zipPath)
		return "ffmpeg"
	}

	_ = os.Remove(zipPath)
	if runtime.GOOS != "windows" {
		_ = os.Chmod(localBin, 0755)
	}
	log.Printf("[FFMPEG-AUTO-INSTALL] Successfully installed FFmpeg to %s!", localBin)
	return localBin
}

func downloadFile(destPath string, url string) error {
	client := &http.Client{Timeout: 5 * time.Minute}
	resp, err := client.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("unexpected HTTP status %d", resp.StatusCode)
	}

	out, err := os.Create(destPath)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, resp.Body)
	return err
}

func extractFFmpegFromZip(zipPath, destPath string) error {
	r, err := zip.OpenReader(zipPath)
	if err != nil {
		return err
	}
	defer r.Close()

	targetSuffix := "ffmpeg"
	if runtime.GOOS == "windows" {
		targetSuffix = "ffmpeg.exe"
	}

	for _, f := range r.File {
		if f.FileInfo().IsDir() {
			continue
		}
		base := strings.ToLower(filepath.Base(f.Name))
		if base == targetSuffix {
			rc, err := f.Open()
			if err != nil {
				return err
			}

			outFile, err := os.OpenFile(destPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0755)
			if err != nil {
				rc.Close()
				return err
			}

			_, err = io.Copy(outFile, rc)
			rc.Close()
			outFile.Close()
			return err
		}
	}
	return os.ErrNotExist
}
