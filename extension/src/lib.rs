
use std::{
    collections::VecDeque,
    mem::size_of,
    os::raw::{c_char, c_int},
};

use timescale_extension_utils::{
    datum::ToDatum,
    elog,
    elog::Level::Error,
    pg_agg,
    pg_sys::{
        ArrayType,
        Datum,
        FLOAT8OID,
        FLOAT8PASSBYVAL,
        TimestampTz,
        construct_md_array,
    },
};

type Milliseconds = i64;
type Microseconds = i64;
const USECS_PER_SEC: i64 = 1_000_000;
const USECS_PER_MS: i64 = 1_000;

pg_agg!{
    // prom divides time into sliding windows of fixed size, e.g.
    // |  5 seconds  |  5 seconds  |  5 seconds  |  5 seconds  |  5 seconds  |
    // we take the first and last values in that bucket and uses `last-first` as the
    // value for that bucket.
    //  | a b c d e | f g h i | j   k |   m    |
    //  |   e - a   |  i - f  | k - j | <null> |
    pub fn gapfill_delta_transition(
        state: Option<Pox<GapfillDeltaTransition>>,
        lowest_time: TimestampTz,
        greatest_time: TimestampTz,
        step_size: Milliseconds, // `prev_now - step` is where the next window starts
        range: Milliseconds, // the size of a window to delta over
        time: TimestampTz,
        val: f64,
    ) -> Option<Pox<GapfillDeltaTransition>> {
        if time < lowest_time || time > greatest_time {
            elog!(Error, "input time less than lowest time")
        }

        let mut state = state.unwrap_or_else(|| {
            let state: Pox<_> = GapfillDeltaTransition::new(
                lowest_time,
                greatest_time,
                range,
                step_size,
                false,
                false,
            ).into();
            state
        });

        state.add_data_point(time, val);

        Some(state)
    }

    pub fn gapfill_rate_transition(
        state: Option<Pox<GapfillDeltaTransition>>,
        lowest_time: TimestampTz,
        greatest_time: TimestampTz,
        step_size: Milliseconds,
        range: Milliseconds, // the size of a window to calculate over
        time: TimestampTz,
        val: f64,
    ) -> Option<Pox<GapfillDeltaTransition>> {
        if time < lowest_time || time > greatest_time {
            elog!(Error, "input time less than lowest time")
        }

        let mut state = state.unwrap_or_else(|| {
            let state: Pox<_> = GapfillDeltaTransition::new(
                lowest_time,
                greatest_time,
                range,
                step_size,
                true,
                true,
            ).into();
            state
        });

        state.add_data_point(time, val);

        Some(state)
    }

    pub fn gapfill_increase_transition(
        state: Option<Pox<GapfillDeltaTransition>>,
        lowest_time: TimestampTz,
        greatest_time: TimestampTz,
        step_size: Milliseconds, // `prev_now - step` is where the next window starts
        range: Milliseconds, // the size of a window to delta over
        time: TimestampTz,
        val: f64,
    ) -> Option<Pox<GapfillDeltaTransition>> {
        if time < lowest_time || time > greatest_time {
            elog!(Error, "input time less than lowest time")
        }

        let mut state = state.unwrap_or_else(|| {
            let state: Pox<_> = GapfillDeltaTransition::new(
                lowest_time,
                greatest_time,
                range,
                step_size,
                true,
                false,
            ).into();
            state
        });

        state.add_data_point(time, val);

        Some(state)
    }

    pub fn gapfill_delta_final(state: Option<Pox<GapfillDeltaTransition>>) -> Option<*mut ArrayType> {
        state.map(|mut s| s.to_pg_array())
    }
}

#[derive(Debug)]
struct GapfillDeltaTransition {
    window: VecDeque<(TimestampTz, f64)>,
    // a Datum for each index in the array, 0 by convention if the value is NULL
    deltas: Vec<Datum>,
    // true if a given value in the array is null, length equal to the number of value
    nulls: Vec<bool>,
    current_window_max: TimestampTz,
    current_window_min: TimestampTz,
    step_size: Microseconds,
    range: Milliseconds,
    greatest_time: TimestampTz,
    is_counter: bool,
    is_rate: bool,
}

impl GapfillDeltaTransition {
    pub fn new(
        lowest_time: TimestampTz,
        greatest_time: TimestampTz,
        range: Milliseconds,
        step_size: Milliseconds,
        is_counter: bool,
        is_rate: bool
    ) -> Self {
        let mut expected_deltas = (greatest_time - lowest_time) / (step_size * USECS_PER_MS);
        if (greatest_time - lowest_time) % (step_size * USECS_PER_MS) != 0 {
            expected_deltas += 1
        }
        GapfillDeltaTransition{
            window: VecDeque::default(),
            deltas: Vec::with_capacity(expected_deltas as usize),
            nulls: Vec::with_capacity(expected_deltas as usize),
            current_window_max: lowest_time + range*USECS_PER_MS,
            current_window_min: lowest_time,
            step_size: step_size*USECS_PER_MS,
            range: range*USECS_PER_MS,
            greatest_time,
            is_counter,
            is_rate,
        }
    }

    fn add_data_point(&mut self, time: TimestampTz, val: f64) {
        // skip stale NaNs
        const STALE_NAN: u64 = 0x7ff0000000000002;
        if val.to_bits() == STALE_NAN {
            return
        }

        while !self.in_current_window(time) {
            self.flush_current_window()
        }

        if self.window.back().map_or(false, |(prev, _)| *prev > time) {
            elog!(Error, "inputs must be in ascending time order")
        }
        if time >= self.current_window_min {
            self.window.push_back((time, val));
        }
    }

    fn in_current_window(&self, time: TimestampTz) -> bool {
        time <= self.current_window_max
    }

    fn flush_current_window(&mut self) {
        self.add_delta_for_current_window();

        self.current_window_min += self.step_size;
        self.current_window_max += self.step_size;

        let current_window_min = self.current_window_min;
        while self.window.front().map_or(false, |(time, _)| *time < current_window_min) {
            self.window.pop_front();
        }
    }

    //based on extrapolatedRate
    // https://github.com/prometheus/prometheus/blob/e5ffa8c9a08a5ee4185271c8c26051ddc1388b7a/promql/functions.go#L59
    fn add_delta_for_current_window(&mut self) {
        if self.window.len() < 2 {
            // if there are 1 or fewer values in the window, store NULL
            self.deltas.push(0);
            self.nulls.push(true);
            return
        }

        let mut counter_correction = 0.0;
        if self.is_counter {
            let mut last_value = 0.0;
            for (_, sample) in &self.window {
                if *sample < last_value {
                    counter_correction += last_value
                }
                last_value = *sample
            }
        }

        let (latest_time, latest_val)= self.window.back().cloned().unwrap();
        let (earliest_time, earliest_val) = self.window.front().cloned().unwrap();
        let mut result_val = latest_val - earliest_val + counter_correction;

        // all calculated durations and interval are in seconds
        let mut duration_to_start = (earliest_time - self.current_window_min) as f64
            / USECS_PER_SEC as f64;
        let duration_to_end = (self.current_window_max - latest_time)  as f64
            / USECS_PER_SEC as f64;

        let sampled_interval = (latest_time - earliest_time) as f64
            / USECS_PER_SEC as f64;
        let avg_duration_between_samples = sampled_interval as f64
            / (self.window.len()-1) as f64;

        if self.is_counter && result_val > 0.0 && earliest_val >= 0.0 {
            // Counters cannot be negative. If we have any slope at
            // all (i.e. result_val went up), we can extrapolate
            // the zero point of the counter. If the duration to the
            // zero point is shorter than the durationToStart, we
            // take the zero point as the start of the series,
            // thereby avoiding extrapolation to negative counter
            // values.
            let duration_to_zero = sampled_interval * (earliest_val / result_val);
            if duration_to_zero < duration_to_start {
                duration_to_start = duration_to_zero
            }
        }

        // If the first/last samples are close to the boundaries of the range,
        // extrapolate the result. This is as we expect that another sample
        // will exist given the spacing between samples we've seen thus far,
        // with an allowance for noise.

        let extrapolation_threshold = avg_duration_between_samples * 1.1;
        let mut extrapolate_to_interval = sampled_interval;

        if duration_to_start < extrapolation_threshold {
            extrapolate_to_interval += duration_to_start
        } else {
            extrapolate_to_interval += avg_duration_between_samples / 2.0
        }

        if duration_to_end < extrapolation_threshold {
            extrapolate_to_interval += duration_to_end
        } else {
            extrapolate_to_interval += avg_duration_between_samples / 2.0
        }

        result_val = result_val * (extrapolate_to_interval / sampled_interval);

        if self.is_rate {
            result_val = result_val / self.range as f64
        }

        self.deltas.push(result_val.to_datum());
        self.nulls.push(false);
    }

    pub fn to_pg_array(&mut self) -> *mut ArrayType {
        while self.current_window_max <= self.greatest_time {
            self.flush_current_window();
        }
        unsafe {
            construct_md_array(
                self.deltas.as_mut_ptr(),
                self.nulls.as_mut_ptr(),
                1,
                &mut (self.nulls.len() as c_int),
                &mut 1,
                FLOAT8OID,
                size_of::<f64>() as c_int,
                FLOAT8PASSBYVAL != 0,
                'd' as u8 as c_char,
            )
        }
    }
}

