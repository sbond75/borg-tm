package internal

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/pkg/errors"
)

const tmUtilCmd = "tmutil"

const (
	unrecognizedSnapshotName backupErr = "unrecognized snapshot format"
)

type backupErr string

func (b backupErr) Error() string {
	return string(b)
}

type BorgBackup struct {
	lockFile             string
	borgArgs             []string
	mountpoints          []string
	useExistingSnapshots bool
	sources              []string
	snapshotsToUse       []string
	backupName           string
	dryRun               bool
}

func NewBackup(mountpoints []string, lockfile string, useExistingSnapshots bool, sources []string, snapshotsToUse []string, backupName string, borgArgs []string, dryRun bool) BorgBackup {
	return BorgBackup{
		lockFile:             lockfile,
		borgArgs:             borgArgs,
		mountpoints:          mountpoints,
		useExistingSnapshots: useExistingSnapshots,
		sources:              sources,
		snapshotsToUse:       snapshotsToUse,
		backupName:           backupName,
		dryRun:               dryRun,
	}
}

func (b BorgBackup) getFileLock() error {
	file, err := os.OpenFile(b.lockFile, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return errors.Wrap(err, "error while opening lockfile")
	}
	for {
		err = syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err != syscall.EINTR {
			break
		}
	}
	err = errors.Wrap(err, "error while acquiring file lock (maybe another process running?)")
	return err
}

func (b BorgBackup) Run(ctx context.Context) (finalErr error) {
	var snapshots []string
	innerFunc := func() error {
		err := b.getFileLock()
		if err != nil {
			return err
		}
		if !b.useExistingSnapshots {
			// https://www.tutorialspoint.com/how-to-handle-errors-within-waitgroups-in-golang , https://medium.com/swlh/using-goroutines-and-wait-groups-for-concurrency-in-golang-78ca7a069d28
			fatalErrorChannel := make(chan error)
			wgDone := make(chan bool)
			var wg sync.WaitGroup
			wg.Add(len(b.sources))

			for i := 0; i < len(b.sources); i++ {
				source := b.sources[i]

				go func(source string) {
					fmt.Printf("Creating snapshot for source %s\n", source)
					err = b.createSnapshot(source)
					if err != nil {
						err = errors.Wrapf(err, "error while creating snapshot for source %s", source)

						// return err
						fatalErrorChannel <- err
					} else {
						fmt.Printf("Created snapshot for source %s\n", source)
					}
					wg.Done()
				}(source)
			}
			go func() {
				wg.Wait()
				close(wgDone)
			}()

			// "The select statement is used for listening to errors or the WaitGroup to complete." ( https://www.tutorialspoint.com/how-to-handle-errors-within-waitgroups-in-golang )
			select {
			case <-wgDone:
				break
			case err := <-fatalErrorChannel:
				close(fatalErrorChannel)
				// log.Fatal("Error encountered: ", err)
				return err
			}
		}
		snapshots = []string{}
		for i := 0; i < len(b.sources); i++ {
			source := b.sources[i]
			mountpoint := b.mountpoints[i]

			fmt.Printf("source: %s\n", source)
			fmt.Printf("mountpoint: %s\n", mountpoint)
			shouldMount := source != mountpoint
			var snapshot string = ""
			var err error = nil
			if shouldMount && (len(b.snapshotsToUse) == 0 || b.snapshotsToUse[i] == "") {
				snapshot, err = b.getLatestSnapshot(source)
			} else if len(b.snapshotsToUse) > 0 {
				snapshot = b.snapshotsToUse[i]
			}
			if err != nil {
				return err
			}
			if shouldMount {
				err = b.mountSnapshot(snapshot, source, mountpoint)
			}
			if err != nil {
				return err
			}
			defer func() { // "defer will move the execution of the statement to the very end" [of] "a function." ( https://www.educative.io/answers/what-is-the-defer-keyword-in-golang#:~:text=In%20Golang%2C%20the%20defer%20keyword,very%20end%20inside%20a%20function. )
				if shouldMount {
					fmt.Printf("Unmounting %s\n", mountpoint)
					err := unmount(mountpoint)
					if err != nil {
						log.Fatalf("unmount %s failed, need manual cleanup.\n", mountpoint)
					} else {
						fmt.Printf("Unmounted %s\n", mountpoint)
					}
				}
			}()
			snapshots = append(snapshots, snapshot)
		}
		partsArray := [][]string{}
		var now string = time.Now().Format("2006-01-02 15:04:05")
		for i := 0; i < len(snapshots); i++ {
			snapshot := snapshots[i]
			if snapshot == "" {
				snapshot = now // just using `snapshot` variable as a backup display name at this point, since the snapshot is already mounted
				parts := []string{"", "", "", snapshot, ""}
				fmt.Printf("Parts for %s: %s\n", snapshot, strings.Join(parts, `', '`))
				partsArray = append(partsArray, parts)
				continue
			}
			
			// parts := strings.Split(snapshot, ".")
			// if len(parts) != 5 {
			// 	return errors.WithStack(unrecognizedSnapshotName)
			// }
			parts := strings.Split(snapshot, ".")
			if len(parts) != 5 {
				parts = []string{"", "", "", parts[0], ""}
				// return errors.WithStack(unrecognizedSnapshotName)
			}
			fmt.Printf("Parts for %s: %s\n", snapshot, strings.Join(parts, `', '`))
			partsArray = append(partsArray, parts)
		}
		hostName, err := os.Hostname()
		if err != nil {
			return errors.Wrap(err, "error while getting hostname")
		}

		var backupName string = b.backupName
		if backupName == "" {
			backupName = partsArray[0][3]+"@"+hostName
		}
		err = b.invokeBorg(ctx, backupName)
		// if err != nil {
		// 	err2 := removeSnapshots()
		// 	return errors.Errorf("Failed to invoke Borg and also to delete snapshots: %w ; %w", err, err2)
		// } else {
		// 	return removeSnapshots()
		// }
		return err
	}

	removeSnapshots := func() error {
		if b.useExistingSnapshots {
			return nil
		}

		for i := 0; i < len(snapshots); i++ {
			snapshot := snapshots[i]
			source := b.sources[i]

			fmt.Printf("Removing snapshot %s for source %s\n", snapshot, source)
			err := b.removeSnapshot(snapshot, source)
			if err != nil {
				err = errors.Wrapf(err, "error while removing snapshot %s", snapshot)
				if finalErr != nil {
					finalErr = errors.Wrapf(err, "previous error: %w", finalErr)
					return finalErr
				}
				finalErr = err
				return err
			} else {
				fmt.Printf("Removed snapshot %s for source %s\n", snapshot, source)
			}
		}
		return nil
	}

	defer removeSnapshots() // Sets `finalErr` if needed
	finalErr = innerFunc()
	return // (returns `finalErr` -- https://stackoverflow.com/questions/37248898/how-does-defer-and-named-return-value-work )
}

func (b BorgBackup) createSnapshot(source string) error {
	// cmd := exec.Command(tmUtilCmd, "localsnapshot")
	// cmd := exec.Command(tmUtilCmd, "snapshot", source)
	cmd := exec.Command("./apfs/snapUtil", "-c", time.Now().Format("2006-01-02 15:04:05"), source) // Need "com.apple.developer.vfs.snapshot" entitlement
	cmd.Env = safeEnvs()
	err := errors.Wrap(cmd.Run(), "error while creating snapshot")
	return err
}

func (b BorgBackup) getLatestSnapshot(source string) (string, error) {
	cmd := exec.Command(tmUtilCmd, "listlocalsnapshots", source)
	buf := new(bytes.Buffer)
	cmd.Stdout = buf
	cmd.Env = safeEnvs()
	err := errors.Wrap(cmd.Run(), "error while getting latest snapshot")
	if err != nil {
		return "", err
	}
	var lastSnapshotName string
	sc := bufio.NewScanner(buf)
	for sc.Scan() {
		lastSnapshotName = sc.Text()
	}
	if err := sc.Err(); err != nil {
		return "", errors.Wrap(err, "error while finding latest snapshot")
	}
	if lastSnapshotName == "" {
		return "", errors.New("no available snapshots")
	}
	return lastSnapshotName, nil
}

func (b BorgBackup) mountSnapshot(snapshot string, source string, mountpoint string) error {
	// there'is no unix.Mount for Darwin, so we have to
	// use exec to invoke mount.
	// cmd := exec.Command("mount", "-t", "apfs", "-r", "-o", "-s="+snapshot, b.source, mountpoint)
	args := []string{"mount_apfs", "-o", "ro,nobrowse", "-s", snapshot, source, mountpoint}
	fmt.Println(strings.Join(args, `', '`))
	cmd := exec.Command(args[0], args[1:]...)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	cmd.Env = safeEnvs()
	return errors.Wrap(cmd.Run(), "error while mounting snapshot")
}

func (b BorgBackup) removeSnapshot(name string, source string) error {
	// parts := strings.Split(name, ".")
	// if len(parts) != 5 {
	// 	//parts = []string{"", "", "", parts[0], ""}
	// 	return errors.WithStack(unrecognizedSnapshotName)
	// }
	// cmd := exec.Command(tmUtilCmd, "deletelocalsnapshots", parts[3])
	cmd := exec.Command("./apfs/snapUtil", "-d" /*parts[3]*/, name, source)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	cmd.Env = safeEnvs()
	return errors.Wrap(cmd.Run(), "error while removing snapshot "+name)
}

func unmount(mountpoint string) error {
	err := syscall.Unmount(mountpoint, 0)
	return errors.Wrap(err, "error while unmounting")
}

func (b BorgBackup) invokeBorg(ctx context.Context, archiveName string) error {
	args := []string{"create"}
	args = append(args, b.borgArgs...)
	args = append(args, "::"+archiveName)
	args = append(args, b.mountpoints...)
	fmt.Println("borg", args)
	if b.dryRun {
		return nil
	}
	cmd := exec.Command("borg", args...)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	err := cmd.Start()
	if err != nil {
		return errors.Wrap(err, "error while starting borg")
	}
	var interrupted bool
	go func() {
		<-ctx.Done()
		cmd.Process.Signal(syscall.SIGINT)
		interrupted = true
	}()
	err = cmd.Wait()
	if err != nil && !interrupted {
		return errors.Wrap(err, "error while running borg")
	}
	return nil
}

// remove borg related environment variables
func safeEnvs() []string {
	envs := os.Environ()
	envs = append(envs, "BORG_PASSPHRASE=", "BORG_REPO=")
	return envs
}
