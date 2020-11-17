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
	"context"
	"flag"
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/io/synthetic"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
	"github.com/apache/beam/sdks/go/pkg/beam/x/beamx"
	"github.com/apache/beam/sdks/go/test/load"
)

var (
	iterations                = flag.Int("iterations", 1, "A number of subsequent ParDo transforms to be performed")
	numberOfCounters          = flag.Int("number_of_counters", 0, "A number of counter metrics to be created for each DoFn transform")
	numberOfCounterOperations = flag.Int("number_of_counter_operations", 0, "A number of operations on counters to be performed in each DoFn")
	syntheticSourceConfig     = flag.String("input_options", "", "A JSON object that describes the configuration for synthetic source")
)

func init() {
	beam.RegisterType(reflect.TypeOf((*counterOperationFn)(nil)).Elem())
}

type counterOperationFn struct {
	numberOfCounterOperations int
	counters                  []beam.Counter
}

func (fn *counterOperationFn) Setup() {
	fn.numberOfCounterOperations = *numberOfCounterOperations
	fn.counters = make([]beam.Counter, 0)
	for i := 0; i < *numberOfCounters; i++ {
		fn.counters = append(fn.counters, beam.NewCounter("counterOperationFn", fmt.Sprint("counter-", i)))
	}
}

func (fn *counterOperationFn) ProcessElement(ctx context.Context, key []byte, value []byte, emit func([]byte, []byte)) {
	for i := 0; i < fn.numberOfCounterOperations; i++ {
		for _, counter := range fn.counters {
			counter.Inc(ctx, 1)
		}
	}
	emit(key, value)
}

func parseSyntheticSourceConfig() synthetic.SourceConfig {
	if *syntheticSourceConfig == "" {
		panic("--input_options not provided")
	} else {
		encoded := []byte(*syntheticSourceConfig)
		return synthetic.DefaultSourceConfig().BuildFromJSON(encoded)
	}
}

func main() {
	flag.Parse()
	beam.Init()
	ctx := context.Background()

	p, s := beam.NewPipelineWithRoot()

	src := synthetic.SourceSingle(s, parseSyntheticSourceConfig())

	pcoll := beam.ParDo(s, &load.MeasureTime{}, src)

	for i := 0; i < *iterations; i++ {
		pcoll = beam.ParDo(s, &counterOperationFn{}, pcoll)
	}

	presult, err := beamx.RunWithMetrics(ctx, p)
	if err != nil {
		log.Fatalf(ctx, "Failed to execute job: %v", err)
	}

	if presult != nil {
		metrics := presult.Metrics().AllMetrics()
		load.PublishMetrics(ctx, metrics)
	}
}
