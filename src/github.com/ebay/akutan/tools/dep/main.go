// Copyright 2019 eBay Inc.
// Primary authors: Simon Fell, Diego Ongaro,
//                  Raymond Kroeker, and Sathish Kandasamy.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Command dep checks / fetches / update dependencies
package main

// as this tool is installing all the dependencies it can't depend on
// anything other than the standard library.
import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Dep defines an external repository that Akutan requires.
type Dep struct {
	repo    string
	dir     string
	version string
}

var options struct {
	jobs int
}

func fatalf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}

func main() {
	flag.IntVar(&options.jobs, "j", 16, "Number of repos to fetch in parallel")
	flag.Parse()

	checkDups()

	var waitGroup sync.WaitGroup
	ch := make(chan Dep, options.jobs)

	// Start fetch jobs
	for i := 0; i < options.jobs; i++ {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			for dep := range ch {
				err := update(dep)
				if err != nil {
					fatalf("Error updating %v: %v", dep.repo, err)
				}
			}
		}()
	}

	// Send dependencies to fetchers
	for _, dep := range deps {
		ch <- dep
	}
	close(ch)

	// Wait for fetchers to complete
	waitGroup.Wait()
}

func checkDups() {
	set := make(map[string]bool, len(deps))
	for _, dep := range deps {
		if dep.dir == "" {
			fatalf("Dir provided for dep %s can't be empty", dep.repo)
		}
		if set[dep.dir] {
			fatalf("Dir provided by more than one dependency entries: %v", dep.dir)
		}
		set[dep.dir] = true
	}
}

func update(dep Dep) error {
	preflightDir(dep)
	if !haveTag(dep) && !haveHash(dep) {
		// Retry updating dependencies if there are any failure due to
		// intermittent network issues with the CI.
		err := retry(3, time.Second, func() error { return fetchOrClone(dep) })
		if err != nil {
			return err
		}
	}
	return reset(dep)
}

func haveTag(dep Dep) bool {
	cmd := exec.Command("git", "-C", dep.dir, "show-ref", "--hash", "--tags", dep.version)
	out, err := cmd.Output()
	if err != nil {
		return false
	}
	commit := string(out)
	if len(commit) >= 40 {
		fmt.Printf("Already have %v tag %v (%v)\n", dep.repo, dep.version, commit[:7])
		return true
	}
	return false
}

func haveHash(dep Dep) bool {
	if len(dep.version) < 3 {
		return false
	}
	cmd := exec.Command("git", "-C", dep.dir, "rev-parse", "--verify",
		fmt.Sprintf("%s^{commit}", dep.version))
	out, err := cmd.Output()
	if err != nil {
		return false
	}
	commit := string(out)
	if len(commit) >= 40 && strings.HasPrefix(commit, strings.ToLower(dep.version)) {
		fmt.Printf("Already have %v commit %v\n", dep.repo, commit[:7])
		return true
	}
	return false
}

// preflightDir verifies that the directory specified in the Dep is in a state we can
// safely proceed with. Some invalid but safe to fix states will be corrected, if theres
// an invalid state this will call log.Fatalf
func preflightDir(dep Dep) {
	// git -C <someDir> <someOp>
	// git will ignore the -C flag if <someDir> exists, but doesn't contain a git repository
	// this can happen when a git clone fails (e.g. you're adding a dep and messed up the repo url)
	// so we check this ourselves first
	dirStat, err := os.Stat(dep.dir)
	if os.IsNotExist(err) {
		return // directory doesn't exist, we're good to go
	} else if err != nil {
		fatalf("Unable to determine state of dir %s: %s", dep.dir, err)
	}
	if !dirStat.IsDir() {
		fatalf("There is a file at the location specified by the dependency.dir %s", dep.dir)
	} else {
		_, err := os.Stat(filepath.Join(dep.dir, ".git"))
		if os.IsNotExist(err) {
			// the dep.dir exists, but doesn't have a .git directory
			// if the dir is empty we can just delete and carry on
			// if it has something in it, we'll just give up and let
			// the user sort it out.
			dirFiles, err := ioutil.ReadDir(dep.dir)
			if err != nil {
				fatalf("Unable to determine contents of directory: %s :%s", dep.dir, err)
			}
			if len(dirFiles) == 0 {
				if err := os.Remove(dep.dir); err != nil {
					fatalf("Unable to delete empty dir at %s: %s", dep.dir, err)
				}
			} else {
				fatalf("Dep dir exists but doesn't contain a git repo, but has other files: %s", dep.dir)
			}
		} else if err != nil {
			fatalf("Unable to determine state of directory %s/.git: %s", dep.dir, err)
		}
	}
}

func fetchOrClone(dep Dep) error {
	cmd := exec.Command("git", "-C", dep.dir,
		"remote", "set-url", "origin", dep.repo)
	err := cmd.Run()
	if err == nil {
		fmt.Printf("Fetching %v (%v)\n", dep.repo, dep.version)
		cmd = exec.Command("git", "-C", dep.dir, "fetch")
	} else {
		fmt.Printf("Cloning %v\n", dep.repo)
		cmd = exec.Command("git", "clone", dep.repo, dep.dir)
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("error calling %v: %v\n%s", cmd.Args, err, out)
	}
	return nil
}

func reset(dep Dep) error {
	cmd := exec.Command("git", "-C", dep.dir, "reset", "--hard", dep.version)
	err := cmd.Run()
	if err != nil {
		return fmt.Errorf("error calling %v: %v", cmd.Args, err)
	}
	return nil
}

// retry invokes function fn and if it returns error, wait for the sleep
// duration before the next attempt, and for each attempt the wait duration
// increases by three times. Returns nil if function fn invocation is
// successful; or error from function fn when it exceeds number of attempts.
func retry(attempts int, sleep time.Duration, fn func() error) error {
	var err error
	for i := 1; i <= attempts; i++ {
		if err = fn(); err == nil {
			break
		}
		fmt.Printf("Warning: call failure (attempt %v of %v): %v\n", i, attempts, err)
		if i < attempts {
			time.Sleep(sleep)
			sleep *= 3
		}
	}
	return err
}
