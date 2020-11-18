// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
    "bytes"
    "context"
    "flag"
    "github.com/apache/beam/sdks/go/pkg/beam"
    "github.com/apache/beam/sdks/go/pkg/beam/io/synthetic"
    "github.com/apache/beam/sdks/go/pkg/beam/log"
    "github.com/apache/beam/sdks/go/pkg/beam/transforms/top"
    "github.com/apache/beam/sdks/go/pkg/beam/x/beamx"
)

var (
    fanout                = flag.Int("fanout", 1, "Fanout")
    topCount              = flag.Int("top_count", 20, "Top count")
    syntheticSourceConfig = flag.String(
        "input_options",
        "{"+
            "\"num_records\": 300, "+
            "\"key_size\": 5, "+
            "\"value_size\": 15, "+
            "\"num_hot_keys\": 30, "+
            "\"hot_key_fraction\": 1.0}",
        "A JSON object that describes the configuration for synthetic source")
)

func parseSyntheticSourceConfig() synthetic.SourceConfig {
    if *syntheticSourceConfig == "" {
        panic("--input_options not provided")
    } else {
        encoded := []byte(*syntheticSourceConfig)
        return synthetic.DefaultSourceConfig().BuildFromJSON(encoded)
    }
}

func CompareLess(key []uint8, value []uint8) bool {
    return bytes.Compare(key, value) < 0
}

func main() {
    flag.Parse()
    beam.Init()

    ctx := context.Background()

    p, s := beam.NewPipelineWithRoot()
    src := synthetic.SourceSingle(s, parseSyntheticSourceConfig())
    for i := 0; i < *fanout; i++ {
        top.LargestPerKey(s, src, *topCount, CompareLess)
    }

    err := beamx.Run(ctx, p)
    if err != nil {
        log.Fatalf(ctx, "Failed to execute job: %v", err)
    }
}
