// Copyright (C) 2018 Nippon Telegraph and Telephone Corporation.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package version

import (
	"fmt"
	"runtime/debug"
)

const (
	MAJOR              uint = 0
	MINOR              uint = 1
	PATCH              uint = 0
	Product                 = "n-gobgp"
	UpstreamRepository      = "osrg/gobgp"
	UpstreamVersion         = "4.10.0"
	// Last upstream commit incorporated into this fork, not the latest available.
	UpstreamCommit = "95c43b005b2901279ce055c17d6f2a6fae329336"
)

var (
	COMMIT     string = ""
	IDENTIFIER string = "dev"
	METADATA   string = ""
	DIRTY      string = ""
)

// Identity describes the producer at capture time. Replay must preserve it.
type Identity struct {
	Product            string
	Version            string
	Commit             string
	Dirty              string
	UpstreamRepository string
	UpstreamVersion    string
	UpstreamCommit     string
}

var revision, modified = readVCS()

func readVCS() (string, string) {
	if info, ok := debug.ReadBuildInfo(); ok && info.Main.Path == "github.com/osrg/gobgp/v4" {
		return vcsSettings(info.Settings)
	}
	return "unknown", "unknown"
}

func vcsSettings(settings []debug.BuildSetting) (string, string) {
	commit, dirty := "unknown", "unknown"
	for _, setting := range settings {
		switch setting.Key {
		case "vcs.revision":
			commit = setting.Value
		case "vcs.modified":
			dirty = setting.Value
		}
	}
	return commit, dirty
}

func Get() Identity {
	commit := revision
	dirty := modified
	if COMMIT != "" {
		commit = COMMIT
	}
	if DIRTY != "" {
		dirty = DIRTY
	}
	return Identity{Product, Version(), commit, dirty, UpstreamRepository, UpstreamVersion, UpstreamCommit}
}

func Description() string {
	i := Get()
	return fmt.Sprintf("%s %s commit=%s dirty=%s upstream=%s@%s upstream-version=%s", i.Product, i.Version, i.Commit, i.Dirty, i.UpstreamRepository, i.UpstreamCommit, i.UpstreamVersion)
}

func Version() string {
	suffix := ""
	if len(IDENTIFIER) > 0 {
		suffix = fmt.Sprintf("-%s", IDENTIFIER)
	}

	if len(COMMIT) > 0 || len(METADATA) > 0 {
		suffix = suffix + "+"
	}

	if len(COMMIT) > 0 {
		suffix = fmt.Sprintf("%s"+"commit.%s", suffix, COMMIT)
	}

	if len(METADATA) > 0 {
		if len(COMMIT) > 0 {
			suffix = suffix + "."
		}
		suffix = suffix + METADATA
	}

	return fmt.Sprintf("%d.%d.%d%s", MAJOR, MINOR, PATCH, suffix)
}
