// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//go:build !unix

package vfs

import "time"

func parasyncProcessCPU() time.Duration { return 0 }
