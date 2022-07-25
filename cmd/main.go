package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/quantumghost/borg-tm/consts"
	"github.com/quantumghost/borg-tm/internal"
)

// https://stackoverflow.com/questions/28322997/how-to-get-a-list-of-values-into-a-flag-in-golang
type arrayFlags []string

func (i *arrayFlags) String() string {
	return "my string representation"
}
func (i *arrayFlags) Set(value string) error {
	*i = append(*i, value)
	return nil
}

func main() {
	var borgArgs, lockFile string
	var mountpoints, sources arrayFlags
	var useExistingSnapshots, dryRun bool
	flag.StringVar(&borgArgs, "borg-args", "", "argument passed to `borg create`")
	// flag.StringVar(&mountpoint, "mountpoint", "/tmp/snapshot",
	// 	"mountpoint for snapshot, should be kept the same across backups")
	flag.Var(&mountpoints, "mountpoint", "mountpoint(s) for snapshot(s), should be kept the same across backups")
	flag.StringVar(&lockFile, "lock-file", "/var/run/borg.lock", "lock file for borg-tm")
	//flag.StringVar(&source, "source", "/", "source to back up")
	flag.Var(&sources, "source", "source(s) to back up")
	flag.BoolVar(&useExistingSnapshots, "use-existing-snapshots", false, "use the latest existing snapshot on the source(s) to back up from. If not provided, will create a snapshot.")
	flag.BoolVar(&dryRun, "dry-run", false, "create and remove snapshots, but don't run borg, only print the borg command that would have been executed.")
	var printVersion bool
	flag.BoolVar(&printVersion, "V", false, "print version")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, `Usage of %s

This program must be run as root.

Environment variables:
- BORG_REPO: repository to backup to
- BORG_PASSPHRASE: passphrase for borg repository

Arguments:
`, os.Args[0])

		flag.PrintDefaults()
		//fmt.Fprintf(os.Stderr, "...And then put sources in a list which will be backed up. For example: `/ /System/Volumes/Data`")
		
		fmt.Fprintf(os.Stderr, "\nNote: %s\n", "`-mountpoint` and `-source` can be used multiple times to set more mountpoints and sources (respective of the order provided for each). For example, use `-source / -source /System/Volumes/Data -mountpoint /tmp/snapshot -mountpoint /tm/snapshot-data` to set two sources each with their corresponding mountpoint.")
	}
	flag.Parse()
	//sources = flag.Args() // https://stackoverflow.com/questions/28322997/how-to-get-a-list-of-values-into-a-flag-in-golang
	if printVersion {
		consts.PrintVersion()
		os.Exit(0)
	}
	if len(mountpoints) == 0 {
		log.Fatalln("Need at least one mountpoint, such as `-mountpoint /tmp/snapshot`")
		os.Exit(1)
	}
	if len(sources) == 0 {
		log.Fatalln("Need at least one source, such as `-source /`")
		os.Exit(1)
	}
	if len(mountpoints) != len(sources) {
		log.Fatalln("The number of mountpoints provided (%d) is not the same as the number of sources provided (%d)", len(mountpoints), len(sources))
		os.Exit(1)
	}

	repo := os.Getenv("BORG_REPO")
	if repo == "" {
		log.Fatalln("BORG_REPO not specified")
	}
	if pass := os.Getenv("BORG_PASSPHRASE"); pass == "" {
		log.Fatalln("BORG_PASSPHRASE not specified")
	}
	parts := strings.Split(borgArgs, " ")
	args := make([]string, 0, len(parts))
	for _, v := range parts {
		if v != "" {
			args = append(args, v)
		}
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	ctx, cancelFn := context.WithCancel(context.Background())
	go func() {
		<-sig
		cancelFn()
	}()

	if os.Getuid() != 0 {
		log.Fatalln("requires root privileges.")
	}
	backup := internal.NewBackup(mountpoints, lockFile, useExistingSnapshots, sources, args, dryRun)
	err := backup.Run(ctx)
	if err != nil {
		log.Fatalf("error while backup: %+v\n", err)
	}
}
