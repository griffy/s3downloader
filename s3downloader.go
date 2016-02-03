package main

import (
    "os"
    "io"
    "time"
    "fmt"
    "net/http"
    "github.com/rlmcpherson/s3gof3r"
    "github.com/gorilla/mux"
)

const (
    PORT = "8090"

    Running = "RUNNING"
    Completed = "COMPLETED"
    Failed = "FAILED"
    Unknown = "UNKNOWN"
)

var defaultDownloadManager *downloadManager

type downloadManager struct {
    manageDuration time.Duration
    downloadIdStarted map[string]time.Time
    downloadIdChannel chan string
    storedDownloadFile map[string]downloadFile
    downloadFileChannel chan downloadFile
    latestDownloadStatus map[string]downloadStatus
    downloadStatusChannel chan downloadStatus
}

type downloadFile struct {
    Id string
    S3Filename string
    Filepath string
}

type downloadStatus struct {
    Id string
    Text string
}

func NewDownloadManager(manageDuration time.Duration) *downloadManager {
    return &downloadManager{
        manageDuration: manageDuration,
        downloadIdStarted: make(map[string]time.Time),
        downloadIdChannel: make(chan string),
        storedDownloadFile: make(map[string]downloadFile),
        downloadFileChannel: make(chan downloadFile),
        latestDownloadStatus: make(map[string]downloadStatus),
        downloadStatusChannel: make(chan downloadStatus),
    }
}

func (t *downloadManager) Manage() {
    for {
        select {
        case id := <-t.downloadIdChannel:
            t.downloadIdStarted[id] = time.Now()
            fmt.Printf("%s: Download started at %s\n", id, t.downloadIdStarted[id])
        case f := <-t.downloadFileChannel:
            fmt.Printf("%s: Local file %s created\n", f.Id, f.Filepath)
            t.storedDownloadFile[f.Id] = f
        case s := <-t.downloadStatusChannel:
            fmt.Printf("%s: Download status changed to %s\n", s.Id, s.Text)
            t.latestDownloadStatus[s.Id] = s
        default:
            for id, started := range t.downloadIdStarted {
                if time.Since(started) >= t.manageDuration {
                    fmt.Printf("%s: Purging memory of download\n", id)

                    delete(t.downloadIdStarted, id)
                    if f, exists := t.storedDownloadFile[id]; exists {
                        fmt.Printf("%s: Removing stored file %s\n", id, f.Filepath)

                        os.Remove(f.Filepath)
                        delete(t.storedDownloadFile, id)
                    }
                    if _, exists := t.latestDownloadStatus[id]; exists {
                        delete(t.latestDownloadStatus, id)
                    }
                }
            }
        }
    }
}

func (t *downloadManager) Download(id, s3AccessKey, s3SecretKey, s3Bucket, s3Filename, localFilepath string) {
    // Mark that we have started the download
    t.downloadIdChannel <- id
    t.downloadStatusChannel <- createDownloadStatus(id, Running)

    // Make a file
    f, err := os.Create(localFilepath)
    if err != nil {
        fmt.Printf("%s: Failed to create local file %s with error: %s\n", id, localFilepath, err.Error())
        t.downloadStatusChannel <- createDownloadStatus(id, Failed + " " + err.Error())
        return
    }

    // Get a reader
    keys := s3gof3r.Keys{
        AccessKey: s3AccessKey,
        SecretKey: s3SecretKey,
    }

    s3 := s3gof3r.New("", keys)

    b := s3.Bucket(s3Bucket)
    reader, _, err := b.GetReader(s3Filename, nil)
    if err != nil {
        fmt.Printf("%s: Failed to create reader for S3 file %s with error: %s\n", id, s3Filename, err.Error())
        os.Remove(localFilepath)
        t.downloadStatusChannel <- createDownloadStatus(id, Failed + " " + err.Error())
        return
    }

    // Stream the data from S3 to disk
    _, err = io.Copy(f, reader)
    if err != nil {
        fmt.Printf("%s: Failed to stream S3 file %s to local file %s with error: %s\n", id, s3Filename, localFilepath, err.Error())
        os.Remove(localFilepath)
        t.downloadStatusChannel <- createDownloadStatus(id, Failed + " " + err.Error())
        return
    }
    err = reader.Close()
    if err != nil {
        fmt.Printf("%s: Failed to finish writing local file %s with error: %s\n", id, localFilepath, err.Error())
        os.Remove(localFilepath)
        t.downloadStatusChannel <- createDownloadStatus(id, Failed + " " + err.Error())
        return
    }

    t.downloadFileChannel <- createDownloadFile(id, s3Filename, localFilepath)
    t.downloadStatusChannel <- createDownloadStatus(id, Completed)
}

func (t *downloadManager) Status(id string) string {
    statusText := Unknown

    if status, exists := t.latestDownloadStatus[id]; exists {
        statusText = status.Text
    }

    return statusText
}

func handleDownload(w http.ResponseWriter, r *http.Request) {
    s3AccessKey := r.FormValue("s3_access_key")
    s3SecretKey := r.FormValue("s3_secret_key")

    s3Bucket := r.FormValue("s3_bucket")
    s3Filename := r.FormValue("s3_filename")
    localFilepath := r.FormValue("local_filepath")
    id := r.FormValue("id")

    fmt.Printf("%s: Received POST to /download with ID %s and S3 filename %s\n", id, id, s3Filename)

    go defaultDownloadManager.Download(id, s3AccessKey, s3SecretKey, s3Bucket, s3Filename, localFilepath)

    w.WriteHeader(http.StatusOK)
}

func handleDownloadStatus(w http.ResponseWriter, r *http.Request) {
    vars := mux.Vars(r)
    id := vars["id"]

    fmt.Printf("%s: Received GET to /download/%s/status\n", id, id)

    status := defaultDownloadManager.Status(id)

    fmt.Printf("%s: Returning status %s\n", id, status)

    w.Write([]byte(status))
}

func createDownloadStatus(id, statusText string) downloadStatus {
    return downloadStatus{
        Id: id,
        Text: statusText,
    }
}

func createDownloadFile(id, s3Filename, localFilepath string) downloadFile {
    return downloadFile{
        Id: id,
        S3Filename: s3Filename,
        Filepath: localFilepath,
    }
}

func main() {
    s3gof3r.DefaultConfig.Md5Check = false

    defaultDownloadManager = NewDownloadManager(24 * time.Hour)
    go defaultDownloadManager.Manage()

    router := mux.NewRouter()
    router.HandleFunc("/download", handleDownload)
    router.HandleFunc("/download/{id}/status", handleDownloadStatus)

    http.Handle("/", router)

    fmt.Println("Starting up S3 download manager...")

    http.ListenAndServe(":" + PORT, nil)
}