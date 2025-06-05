// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use datafusion::{
    common::instant::Instant,
    physical_plan::metrics::{Count, ExecutionPlanMetricsSet, MetricBuilder, Time},
};

/// A timer that can be started and stopped.
pub struct StartableTime {
    pub(crate) metrics: Time,
    // use for record each part cost time, will eventually add into 'metrics'.
    pub(crate) start: Option<Instant>,
}

impl StartableTime {
    pub(crate) fn start(&mut self) {
        assert!(self.start.is_none());
        self.start = Some(Instant::now());
    }

    pub(crate) fn stop(&mut self) {
        if let Some(start) = self.start.take() {
            self.metrics.add_elapsed(start);
        }
    }
}

pub(crate) struct FlightStreamMetrics {
    pub time_processing: StartableTime,
    pub time_reading_total: StartableTime,
    pub poll_count: Count,
    pub output_rows: Count,
    pub bytes_decoded: Count,
    pub time_pending: StartableTime,          // New: Track total time spent in Pending state
    pub time_schema_mapping: StartableTime,   // New: Track time spent in schema mapping
    pub pending_count: Count,                 // New: Count number of Poll::Pending occurrences
    pub batch_processing_times: Vec<f64>,     // New: Track individual batch processing times
    pub last_pending_start: Option<Instant>,  // New: Track when we entered Pending state
    pub get_stream_time: StartableTime,
    pub processing_stream_time: StartableTime,
    pub init_stream_time: StartableTime,
    pub last_get_stream: Option<Instant>,
    pub last_processing_stream_time: Option<Instant>,
    pub last_init_stream_time: Option<Instant>
}

impl FlightStreamMetrics {
    pub(crate) fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        Self {
            time_processing: StartableTime {
                metrics: MetricBuilder::new(metrics).subset_time("time_processing", partition),
                start: None,
            },
            time_reading_total: StartableTime {
                metrics: MetricBuilder::new(metrics).subset_time("time_reading_total", partition),
                start: None,
            },
            output_rows: MetricBuilder::new(metrics).output_rows(partition),
            poll_count: MetricBuilder::new(metrics).counter("poll_count", partition),
            bytes_decoded: MetricBuilder::new(metrics).counter("bytes_decoded", partition),
            time_pending: StartableTime {
                metrics: MetricBuilder::new(metrics).subset_time("time_pending", partition),
                start: None,
            },
            time_schema_mapping: StartableTime {
                metrics: MetricBuilder::new(metrics).subset_time("time_schema_mapping", partition),
                start: None,
            },
            pending_count: MetricBuilder::new(metrics).counter("pending_count", partition),
            batch_processing_times: Vec::new(),
            last_pending_start: None,
            get_stream_time: StartableTime {
                metrics: MetricBuilder::new(metrics).subset_time("get_stream_time", partition),
                start: None,
            },
            processing_stream_time: StartableTime {
                metrics: MetricBuilder::new(metrics).subset_time("processing_stream_time", partition),
                start: None,
            },
            init_stream_time: StartableTime {
                metrics: MetricBuilder::new(metrics).subset_time("init_stream_time", partition),
                start: None,
            },
            last_get_stream: None,
            last_processing_stream_time: None,
            last_init_stream_time: None
        }
    }

    pub fn start_get_stream(&mut self) {
        if self.get_stream_time.start.is_none() {
            self.get_stream_time.start();
            self.last_get_stream = Some(Instant::now());
        }
    }

    pub fn end_get_stream(&mut self) {
        if self.last_get_stream.is_some() {
            self.get_stream_time.stop();
            self.last_get_stream = None;
        }
    }

    pub fn start_processing_stream_time(&mut self) {
        if self.processing_stream_time.start.is_none() {
            self.processing_stream_time.start();
            self.last_processing_stream_time = Some(Instant::now());
        }
    }

    pub fn end_processing_stream_time(&mut self) {
        if self.last_processing_stream_time.is_some() {
            self.processing_stream_time.stop();
            self.last_processing_stream_time = None;
        }
    }

    pub fn start_init_stream_time(&mut self) {
        if self.init_stream_time.start.is_none() {
            self.init_stream_time.start();
            self.last_init_stream_time = Some(Instant::now());
        }
    }

    pub fn end_init_stream_time(&mut self) {
        if self.last_init_stream_time.is_some() {
            self.init_stream_time.stop();
            self.last_init_stream_time = None;
        }
    }

    // New helper methods
    pub fn start_pending(&mut self) {
        self.pending_count.add(1);
        if self.time_pending.start.is_none() {
            self.time_pending.start();
            self.last_pending_start = Some(Instant::now());
        }
    }

    pub fn stop_pending(&mut self) {
        if self.last_pending_start.is_some() {
            self.time_pending.stop();
            self.last_pending_start = None;
        }
    }

    pub fn record_batch_time(&mut self, duration: f64) {
        self.batch_processing_times.push(duration);
    }

    // New method to get statistics about pending times
    pub fn get_pending_stats(&self) -> PendingStats {
        PendingStats {
            total_pending_time: self.time_pending.metrics.value() as f64,
            pending_count: self.pending_count.value(),
            avg_pending_time: self.time_pending.metrics.value() as f64 / self.pending_count.value() as f64,
            pending_ratio: (self.time_pending.metrics.value() / self.time_reading_total.metrics.value()) as f64,
        }
    }
}


// New struct to hold pending statistics
pub struct PendingStats {
    pub total_pending_time: f64,
    pub pending_count: usize,
    pub avg_pending_time: f64,
    pub pending_ratio: f64,
}









