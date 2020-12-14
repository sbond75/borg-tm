package internal

import (
	"bufio"
	"bytes"
	"context"
	"log"
	"os"
	"os/exec"
	"strings"
	"syscall"

	"github.com/pkg/errors"

	"fmt"
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
	lockFile   string
	borgArgs   []string
	mountpoint string
	source     string
}

func NewBackup(mountpoint string, lockfile string, source string, borgArgs []string) BorgBackup {
	return BorgBackup{
		lockFile:   lockfile,
		borgArgs:   borgArgs,
		mountpoint: mountpoint,
		source: source,
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

func (b BorgBackup) Run(ctx context.Context) error {
	err := b.getFileLock()
	if err != nil {
		return err
	}
	err = b.createSnapshot()
	if err != nil {
		return err
	}
	snapshot, err := b.getLatestSnapshot()
	if err != nil {
		return err
	}
	err = b.mountSnapshot(snapshot)
	if err != nil {
		return err
	}
	defer func() {
		err := unmount(b.mountpoint)
		if err != nil {
			log.Fatalf("unmount %s failed, need manual cleanup.\n", b.mountpoint)
		}
	}()
	parts := strings.Split(snapshot, ".")
	if len(parts) != 5 {
		return errors.WithStack(unrecognizedSnapshotName)
	}
	hostName, err := os.Hostname()
	if err != nil {
		return errors.Wrap(err, "error while getting hostname")
	}
	err = b.invokeBorg(ctx, parts[3]+"@"+hostName)
	if err != nil {
		return err
	}
	err = removeSnapshot(snapshot)
	return errors.Wrap(err, "error while removing snapshot")
}

func (b BorgBackup) createSnapshot() error {
	//cmd := exec.Command(tmUtilCmd, "localsnapshot")
	cmd := exec.Command(tmUtilCmd, "snapshot", b.source)
	cmd.Env = safeEnvs()
	err := errors.Wrap(cmd.Run(), "error while creating snapshot")
	return err
}

func (b BorgBackup) getLatestSnapshot() (string, error) {
	cmd := exec.Command(tmUtilCmd, "listlocalsnapshots", b.source)
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

func (b BorgBackup) mountSnapshot(snapshot string) error {
	// there'is no unix.Mount for Darwin, so we have to
	// use exec to invoke mount.
	//cmd := exec.Command("mount", "-t", "apfs", "-r", "-o", "-s="+snapshot, b.source, b.mountpoint)
	args := []string{"mount_apfs", "-o", "rdonly", "-s", snapshot, b.source, b.mountpoint}
	fmt.Print(strings.Join(args, `', '`), '\n')
	cmd := exec.Command("mount_apfs", "-o", "rdonly,nobrowse", "-s", snapshot, b.source, b.mountpoint)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = safeEnvs()
	return errors.Wrap(cmd.Run(), "error while mounting snapshot")
}

func removeSnapshot(name string) error {
	parts := strings.Split(name, ".")
	if len(parts) != 5 {
		return errors.WithStack(unrecognizedSnapshotName)
	}
	cmd := exec.Command(tmUtilCmd, "deletelocalsnapshots", parts[3])
	cmd.Env = safeEnvs()
	return errors.Wrap(cmd.Run(), "error while removing snapshot "+name)
}

func unmount(mountpoint string) error {
	err := syscall.Unmount(mountpoint, 0)
	return errors.Wrap(err, "error while unmounting")
}

func (b BorgBackup) invokeBorg(ctx context.Context, archiveName string) error {
	args := []string{"create", "::" + archiveName, b.mountpoint}
	args = append(args, b.borgArgs...)
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
