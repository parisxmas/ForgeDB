//! SIMD-accelerated functions for hot code paths in ForgeDB.
//!
//! On ARM64 (Apple Silicon), uses NEON intrinsics via `std::arch::aarch64`.
//! On all other architectures, falls back to scalar implementations that
//! are written in a style amenable to auto-vectorization by LLVM.
//!
//! # Functions
//!
//! - **`sum_i32`** / **`sum_i64`**: Vectorized summation of integer slices.
//! - **`min_i32`** / **`max_i32`**: Vectorized min/max of i32 slices.
//! - **`min_i64`** / **`max_i64`**: Vectorized min/max of i64 slices.
//! - **`filter_gt_i32`** / **`filter_lt_i32`**: Build a selection bitmask
//!   for `val > threshold` or `val < threshold` comparisons.
//! - **`count_nonzero_u16_pairs`**: Count slot entries where offset != 0
//!   (used by `count_live_tuples` in the heap page).
//! - **`sum_f64`**: Vectorized summation of f64 slices.

// =========================================================================
// ARM64 NEON intrinsics
// =========================================================================

#[cfg(target_arch = "aarch64")]
mod neon {
    use std::arch::aarch64::*;

    /// Sum a slice of i32 values using NEON SIMD, returning i64 to avoid overflow.
    ///
    /// Processes 4 x i32 per iteration, widening to i64 before accumulating.
    /// Handles the remainder with scalar code.
    ///
    /// # Safety
    /// Requires aarch64 with NEON support (all Apple Silicon has this).
    #[target_feature(enable = "neon")]
    pub unsafe fn sum_i32_neon(data: &[i32]) -> i64 {
        let len = data.len();
        let chunks = len / 4;
        let ptr = data.as_ptr();

        let mut acc = vdupq_n_s64(0);

        for i in 0..chunks {
            let v = vld1q_s32(ptr.add(i * 4));
            // Widen lower and upper halves to i64 and accumulate
            let lo = vmovl_s32(vget_low_s32(v));
            let hi = vmovl_s32(vget_high_s32(v));
            acc = vaddq_s64(acc, lo);
            acc = vaddq_s64(acc, hi);
        }

        // Horizontal sum of the 2-lane i64 accumulator
        let mut result = vgetq_lane_s64(acc, 0) + vgetq_lane_s64(acc, 1);

        // Scalar remainder
        let remainder_start = chunks * 4;
        for i in remainder_start..len {
            result += *data.get_unchecked(i) as i64;
        }
        result
    }

    /// Sum a slice of i64 values using NEON SIMD.
    ///
    /// # Safety
    /// Requires aarch64 with NEON support.
    #[target_feature(enable = "neon")]
    pub unsafe fn sum_i64_neon(data: &[i64]) -> i64 {
        let len = data.len();
        let chunks = len / 2;
        let ptr = data.as_ptr();

        let mut acc = vdupq_n_s64(0);

        for i in 0..chunks {
            let v = vld1q_s64(ptr.add(i * 2));
            acc = vaddq_s64(acc, v);
        }

        let mut result = vgetq_lane_s64(acc, 0) + vgetq_lane_s64(acc, 1);

        // Scalar remainder
        if len % 2 != 0 {
            result += *data.get_unchecked(len - 1);
        }
        result
    }

    /// Find the minimum of a non-empty i32 slice using NEON SIMD.
    ///
    /// # Safety
    /// Requires aarch64 with NEON support. `data` must be non-empty.
    #[target_feature(enable = "neon")]
    pub unsafe fn min_i32_neon(data: &[i32]) -> i32 {
        let len = data.len();
        if len == 0 {
            return i32::MAX;
        }

        let chunks = len / 4;
        let ptr = data.as_ptr();

        if chunks > 0 {
            let mut acc = vld1q_s32(ptr);
            for i in 1..chunks {
                let v = vld1q_s32(ptr.add(i * 4));
                acc = vminq_s32(acc, v);
            }

            // Horizontal min: reduce 4-lane to scalar
            let mut result = vgetq_lane_s32(acc, 0);
            let v1 = vgetq_lane_s32(acc, 1);
            let v2 = vgetq_lane_s32(acc, 2);
            let v3 = vgetq_lane_s32(acc, 3);
            if v1 < result { result = v1; }
            if v2 < result { result = v2; }
            if v3 < result { result = v3; }

            // Scalar remainder
            let remainder_start = chunks * 4;
            for i in remainder_start..len {
                let val = *data.get_unchecked(i);
                if val < result {
                    result = val;
                }
            }
            result
        } else {
            // Fewer than 4 elements: pure scalar
            let mut result = *data.get_unchecked(0);
            for i in 1..len {
                let val = *data.get_unchecked(i);
                if val < result {
                    result = val;
                }
            }
            result
        }
    }

    /// Find the maximum of a non-empty i32 slice using NEON SIMD.
    ///
    /// # Safety
    /// Requires aarch64 with NEON support. `data` must be non-empty.
    #[target_feature(enable = "neon")]
    pub unsafe fn max_i32_neon(data: &[i32]) -> i32 {
        let len = data.len();
        if len == 0 {
            return i32::MIN;
        }

        let chunks = len / 4;
        let ptr = data.as_ptr();

        if chunks > 0 {
            let mut acc = vld1q_s32(ptr);
            for i in 1..chunks {
                let v = vld1q_s32(ptr.add(i * 4));
                acc = vmaxq_s32(acc, v);
            }

            let mut result = vgetq_lane_s32(acc, 0);
            let v1 = vgetq_lane_s32(acc, 1);
            let v2 = vgetq_lane_s32(acc, 2);
            let v3 = vgetq_lane_s32(acc, 3);
            if v1 > result { result = v1; }
            if v2 > result { result = v2; }
            if v3 > result { result = v3; }

            let remainder_start = chunks * 4;
            for i in remainder_start..len {
                let val = *data.get_unchecked(i);
                if val > result {
                    result = val;
                }
            }
            result
        } else {
            let mut result = *data.get_unchecked(0);
            for i in 1..len {
                let val = *data.get_unchecked(i);
                if val > result {
                    result = val;
                }
            }
            result
        }
    }

    /// Build a boolean selection vector: `true` where `data[i] > threshold`.
    ///
    /// Uses NEON `vcgtq_s32` to compare 4 values at once and extracts the
    /// mask bits to determine which rows pass the filter.
    ///
    /// # Safety
    /// Requires aarch64 with NEON support.
    #[target_feature(enable = "neon")]
    pub unsafe fn filter_gt_i32_neon(data: &[i32], threshold: i32, selection: &mut Vec<bool>) {
        let len = data.len();
        selection.clear();
        selection.reserve(len);

        let chunks = len / 4;
        let ptr = data.as_ptr();
        let thresh_vec = vdupq_n_s32(threshold);

        for i in 0..chunks {
            let v = vld1q_s32(ptr.add(i * 4));
            let mask = vcgtq_s32(v, thresh_vec);
            // Extract each lane of the comparison mask
            // Non-zero means the comparison was true
            selection.push(vgetq_lane_u32(mask, 0) != 0);
            selection.push(vgetq_lane_u32(mask, 1) != 0);
            selection.push(vgetq_lane_u32(mask, 2) != 0);
            selection.push(vgetq_lane_u32(mask, 3) != 0);
        }

        // Scalar remainder
        let remainder_start = chunks * 4;
        for i in remainder_start..len {
            selection.push(*data.get_unchecked(i) > threshold);
        }
    }

    /// Build a boolean selection vector: `true` where `data[i] < threshold`.
    ///
    /// # Safety
    /// Requires aarch64 with NEON support.
    #[target_feature(enable = "neon")]
    pub unsafe fn filter_lt_i32_neon(data: &[i32], threshold: i32, selection: &mut Vec<bool>) {
        let len = data.len();
        selection.clear();
        selection.reserve(len);

        let chunks = len / 4;
        let ptr = data.as_ptr();
        let thresh_vec = vdupq_n_s32(threshold);

        for i in 0..chunks {
            let v = vld1q_s32(ptr.add(i * 4));
            let mask = vcltq_s32(v, thresh_vec);
            selection.push(vgetq_lane_u32(mask, 0) != 0);
            selection.push(vgetq_lane_u32(mask, 1) != 0);
            selection.push(vgetq_lane_u32(mask, 2) != 0);
            selection.push(vgetq_lane_u32(mask, 3) != 0);
        }

        let remainder_start = chunks * 4;
        for i in remainder_start..len {
            selection.push(*data.get_unchecked(i) < threshold);
        }
    }

    /// Build a boolean selection vector: `true` where `data[i] >= threshold`.
    ///
    /// # Safety
    /// Requires aarch64 with NEON support.
    #[target_feature(enable = "neon")]
    pub unsafe fn filter_gte_i32_neon(data: &[i32], threshold: i32, selection: &mut Vec<bool>) {
        let len = data.len();
        selection.clear();
        selection.reserve(len);

        let chunks = len / 4;
        let ptr = data.as_ptr();
        let thresh_vec = vdupq_n_s32(threshold);

        for i in 0..chunks {
            let v = vld1q_s32(ptr.add(i * 4));
            let mask = vcgeq_s32(v, thresh_vec);
            selection.push(vgetq_lane_u32(mask, 0) != 0);
            selection.push(vgetq_lane_u32(mask, 1) != 0);
            selection.push(vgetq_lane_u32(mask, 2) != 0);
            selection.push(vgetq_lane_u32(mask, 3) != 0);
        }

        let remainder_start = chunks * 4;
        for i in remainder_start..len {
            selection.push(*data.get_unchecked(i) >= threshold);
        }
    }

    /// Build a boolean selection vector: `true` where `data[i] <= threshold`.
    ///
    /// # Safety
    /// Requires aarch64 with NEON support.
    #[target_feature(enable = "neon")]
    pub unsafe fn filter_lte_i32_neon(data: &[i32], threshold: i32, selection: &mut Vec<bool>) {
        let len = data.len();
        selection.clear();
        selection.reserve(len);

        let chunks = len / 4;
        let ptr = data.as_ptr();
        let thresh_vec = vdupq_n_s32(threshold);

        for i in 0..chunks {
            let v = vld1q_s32(ptr.add(i * 4));
            let mask = vcleq_s32(v, thresh_vec);
            selection.push(vgetq_lane_u32(mask, 0) != 0);
            selection.push(vgetq_lane_u32(mask, 1) != 0);
            selection.push(vgetq_lane_u32(mask, 2) != 0);
            selection.push(vgetq_lane_u32(mask, 3) != 0);
        }

        let remainder_start = chunks * 4;
        for i in remainder_start..len {
            selection.push(*data.get_unchecked(i) <= threshold);
        }
    }

    /// Build a boolean selection vector: `true` where `data[i] == threshold`.
    ///
    /// # Safety
    /// Requires aarch64 with NEON support.
    #[target_feature(enable = "neon")]
    pub unsafe fn filter_eq_i32_neon(data: &[i32], threshold: i32, selection: &mut Vec<bool>) {
        let len = data.len();
        selection.clear();
        selection.reserve(len);

        let chunks = len / 4;
        let ptr = data.as_ptr();
        let thresh_vec = vdupq_n_s32(threshold);

        for i in 0..chunks {
            let v = vld1q_s32(ptr.add(i * 4));
            let mask = vceqq_s32(v, thresh_vec);
            selection.push(vgetq_lane_u32(mask, 0) != 0);
            selection.push(vgetq_lane_u32(mask, 1) != 0);
            selection.push(vgetq_lane_u32(mask, 2) != 0);
            selection.push(vgetq_lane_u32(mask, 3) != 0);
        }

        let remainder_start = chunks * 4;
        for i in remainder_start..len {
            selection.push(*data.get_unchecked(i) == threshold);
        }
    }

    /// Count how many u16 values (at even offsets in a byte slice) are nonzero.
    /// Used by heap_page::count_live_tuples to count slots with offset != 0.
    ///
    /// Reads u16 values from `data` at stride `stride_bytes`, starting at
    /// `start_offset`. Counts how many are nonzero.
    ///
    /// # Safety
    /// Requires aarch64 with NEON support.
    /// `data` must contain enough bytes for all `count` entries.
    #[target_feature(enable = "neon")]
    pub unsafe fn count_nonzero_slots_neon(
        data: &[u8],
        start_offset: usize,
        stride_bytes: usize,
        count: usize,
    ) -> u16 {
        let mut live = 0u16;

        // Extract u16 offsets into a contiguous buffer for SIMD processing.
        // For small counts (< 8), scalar is fine. For larger counts, batch.
        if count >= 8 {
            // Batch extract offsets into a temp buffer
            let mut offsets = Vec::with_capacity(count);
            for i in 0..count {
                let pos = start_offset + i * stride_bytes;
                let offset = u16::from_le_bytes([
                    *data.get_unchecked(pos),
                    *data.get_unchecked(pos + 1),
                ]);
                offsets.push(offset);
            }

            // Process 8 u16 values at a time using NEON
            let chunks = offsets.len() / 8;
            let ptr = offsets.as_ptr();
            let zero = vdupq_n_u16(0);
            let mut acc = vdupq_n_u16(0);

            for i in 0..chunks {
                let v = vld1q_u16(ptr.add(i * 8));
                // Compare not-equal to zero: result is 0xFFFF (all ones) for non-zero
                let mask = vmvnq_u16(vceqq_u16(v, zero));
                // Each lane is 0xFFFF (-1 as u16) for non-zero, 0 for zero.
                // We want to count, so shift right by 15 to get 1 or 0.
                let ones = vshrq_n_u16(mask, 15);
                acc = vaddq_u16(acc, ones);
            }

            // Horizontal sum of 8 lanes
            live += vgetq_lane_u16(acc, 0);
            live += vgetq_lane_u16(acc, 1);
            live += vgetq_lane_u16(acc, 2);
            live += vgetq_lane_u16(acc, 3);
            live += vgetq_lane_u16(acc, 4);
            live += vgetq_lane_u16(acc, 5);
            live += vgetq_lane_u16(acc, 6);
            live += vgetq_lane_u16(acc, 7);

            // Scalar remainder
            let remainder_start = chunks * 8;
            for i in remainder_start..offsets.len() {
                if *offsets.get_unchecked(i) != 0 {
                    live += 1;
                }
            }
        } else {
            // Scalar for small counts
            for i in 0..count {
                let pos = start_offset + i * stride_bytes;
                let offset = u16::from_le_bytes([
                    *data.get_unchecked(pos),
                    *data.get_unchecked(pos + 1),
                ]);
                if offset != 0 {
                    live += 1;
                }
            }
        }

        live
    }

    /// Sum a slice of f64 values using NEON SIMD.
    ///
    /// # Safety
    /// Requires aarch64 with NEON support.
    #[target_feature(enable = "neon")]
    pub unsafe fn sum_f64_neon(data: &[f64]) -> f64 {
        let len = data.len();
        let chunks = len / 2;
        let ptr = data.as_ptr();

        let mut acc = vdupq_n_f64(0.0);

        for i in 0..chunks {
            let v = vld1q_f64(ptr.add(i * 2));
            acc = vaddq_f64(acc, v);
        }

        let mut result = vgetq_lane_f64(acc, 0) + vgetq_lane_f64(acc, 1);

        // Scalar remainder
        if len % 2 != 0 {
            result += *data.get_unchecked(len - 1);
        }
        result
    }

    /// Find the minimum of a non-empty f64 slice using NEON SIMD.
    ///
    /// # Safety
    /// Requires aarch64 with NEON support. `data` must be non-empty.
    #[target_feature(enable = "neon")]
    pub unsafe fn min_f64_neon(data: &[f64]) -> f64 {
        let len = data.len();
        if len == 0 {
            return f64::INFINITY;
        }

        let chunks = len / 2;
        let ptr = data.as_ptr();

        if chunks > 0 {
            let mut acc = vld1q_f64(ptr);
            for i in 1..chunks {
                let v = vld1q_f64(ptr.add(i * 2));
                acc = vminq_f64(acc, v);
            }
            let mut result = vgetq_lane_f64(acc, 0);
            let v1 = vgetq_lane_f64(acc, 1);
            if v1 < result { result = v1; }

            if len % 2 != 0 {
                let last = *data.get_unchecked(len - 1);
                if last < result { result = last; }
            }
            result
        } else {
            *data.get_unchecked(0)
        }
    }

    /// Find the maximum of a non-empty f64 slice using NEON SIMD.
    ///
    /// # Safety
    /// Requires aarch64 with NEON support. `data` must be non-empty.
    #[target_feature(enable = "neon")]
    pub unsafe fn max_f64_neon(data: &[f64]) -> f64 {
        let len = data.len();
        if len == 0 {
            return f64::NEG_INFINITY;
        }

        let chunks = len / 2;
        let ptr = data.as_ptr();

        if chunks > 0 {
            let mut acc = vld1q_f64(ptr);
            for i in 1..chunks {
                let v = vld1q_f64(ptr.add(i * 2));
                acc = vmaxq_f64(acc, v);
            }
            let mut result = vgetq_lane_f64(acc, 0);
            let v1 = vgetq_lane_f64(acc, 1);
            if v1 > result { result = v1; }

            if len % 2 != 0 {
                let last = *data.get_unchecked(len - 1);
                if last > result { result = last; }
            }
            result
        } else {
            *data.get_unchecked(0)
        }
    }
}

// =========================================================================
// Scalar fallback implementations
// =========================================================================

#[allow(dead_code)]
mod scalar {
    /// Sum a slice of i32 values, returning i64 to avoid overflow.
    /// Written as a simple loop for auto-vectorization by LLVM.
    #[inline]
    pub fn sum_i32_scalar(data: &[i32]) -> i64 {
        let mut acc: i64 = 0;
        for &v in data {
            acc += v as i64;
        }
        acc
    }

    /// Sum a slice of i64 values.
    #[inline]
    pub fn sum_i64_scalar(data: &[i64]) -> i64 {
        let mut acc: i64 = 0;
        for &v in data {
            acc = acc.wrapping_add(v);
        }
        acc
    }

    /// Find the minimum of a non-empty i32 slice.
    #[inline]
    pub fn min_i32_scalar(data: &[i32]) -> i32 {
        let mut result = data[0];
        for &v in &data[1..] {
            if v < result {
                result = v;
            }
        }
        result
    }

    /// Find the maximum of a non-empty i32 slice.
    #[inline]
    pub fn max_i32_scalar(data: &[i32]) -> i32 {
        let mut result = data[0];
        for &v in &data[1..] {
            if v > result {
                result = v;
            }
        }
        result
    }

    /// Build a boolean selection vector: `true` where `data[i] > threshold`.
    #[inline]
    pub fn filter_gt_i32_scalar(data: &[i32], threshold: i32, selection: &mut Vec<bool>) {
        selection.clear();
        selection.reserve(data.len());
        for &v in data {
            selection.push(v > threshold);
        }
    }

    /// Build a boolean selection vector: `true` where `data[i] < threshold`.
    #[inline]
    pub fn filter_lt_i32_scalar(data: &[i32], threshold: i32, selection: &mut Vec<bool>) {
        selection.clear();
        selection.reserve(data.len());
        for &v in data {
            selection.push(v < threshold);
        }
    }

    /// Build a boolean selection vector: `true` where `data[i] >= threshold`.
    #[inline]
    pub fn filter_gte_i32_scalar(data: &[i32], threshold: i32, selection: &mut Vec<bool>) {
        selection.clear();
        selection.reserve(data.len());
        for &v in data {
            selection.push(v >= threshold);
        }
    }

    /// Build a boolean selection vector: `true` where `data[i] <= threshold`.
    #[inline]
    pub fn filter_lte_i32_scalar(data: &[i32], threshold: i32, selection: &mut Vec<bool>) {
        selection.clear();
        selection.reserve(data.len());
        for &v in data {
            selection.push(v <= threshold);
        }
    }

    /// Build a boolean selection vector: `true` where `data[i] == threshold`.
    #[inline]
    pub fn filter_eq_i32_scalar(data: &[i32], threshold: i32, selection: &mut Vec<bool>) {
        selection.clear();
        selection.reserve(data.len());
        for &v in data {
            selection.push(v == threshold);
        }
    }

    /// Count nonzero u16 values at strided offsets in a byte slice.
    #[inline]
    pub fn count_nonzero_slots_scalar(
        data: &[u8],
        start_offset: usize,
        stride_bytes: usize,
        count: usize,
    ) -> u16 {
        let mut live = 0u16;
        for i in 0..count {
            let pos = start_offset + i * stride_bytes;
            let offset = u16::from_le_bytes([data[pos], data[pos + 1]]);
            if offset != 0 {
                live += 1;
            }
        }
        live
    }

    /// Sum a slice of f64 values.
    #[inline]
    pub fn sum_f64_scalar(data: &[f64]) -> f64 {
        let mut acc: f64 = 0.0;
        for &v in data {
            acc += v;
        }
        acc
    }

    /// Find the minimum of a non-empty f64 slice.
    #[inline]
    pub fn min_f64_scalar(data: &[f64]) -> f64 {
        let mut result = data[0];
        for &v in &data[1..] {
            if v < result {
                result = v;
            }
        }
        result
    }

    /// Find the maximum of a non-empty f64 slice.
    #[inline]
    pub fn max_f64_scalar(data: &[f64]) -> f64 {
        let mut result = data[0];
        for &v in &data[1..] {
            if v > result {
                result = v;
            }
        }
        result
    }
}

// =========================================================================
// Public API — dispatches to NEON or scalar at compile time
// =========================================================================

/// Sum a slice of i32 values, returning i64 to avoid overflow.
/// Uses NEON SIMD on ARM64, scalar fallback otherwise.
#[inline]
pub fn sum_i32(data: &[i32]) -> i64 {
    if data.is_empty() {
        return 0;
    }
    #[cfg(target_arch = "aarch64")]
    {
        // Safety: all Apple Silicon supports NEON
        unsafe { neon::sum_i32_neon(data) }
    }
    #[cfg(not(target_arch = "aarch64"))]
    {
        scalar::sum_i32_scalar(data)
    }
}

/// Sum a slice of i64 values.
/// Uses NEON SIMD on ARM64, scalar fallback otherwise.
#[inline]
pub fn sum_i64(data: &[i64]) -> i64 {
    if data.is_empty() {
        return 0;
    }
    #[cfg(target_arch = "aarch64")]
    {
        unsafe { neon::sum_i64_neon(data) }
    }
    #[cfg(not(target_arch = "aarch64"))]
    {
        scalar::sum_i64_scalar(data)
    }
}

/// Find the minimum of a non-empty i32 slice.
/// Returns `None` if the slice is empty.
#[inline]
pub fn min_i32(data: &[i32]) -> Option<i32> {
    if data.is_empty() {
        return None;
    }
    #[cfg(target_arch = "aarch64")]
    {
        Some(unsafe { neon::min_i32_neon(data) })
    }
    #[cfg(not(target_arch = "aarch64"))]
    {
        Some(scalar::min_i32_scalar(data))
    }
}

/// Find the maximum of a non-empty i32 slice.
/// Returns `None` if the slice is empty.
#[inline]
pub fn max_i32(data: &[i32]) -> Option<i32> {
    if data.is_empty() {
        return None;
    }
    #[cfg(target_arch = "aarch64")]
    {
        Some(unsafe { neon::max_i32_neon(data) })
    }
    #[cfg(not(target_arch = "aarch64"))]
    {
        Some(scalar::max_i32_scalar(data))
    }
}

/// Build a selection vector for `data[i] > threshold`.
#[inline]
pub fn filter_gt_i32(data: &[i32], threshold: i32, selection: &mut Vec<bool>) {
    #[cfg(target_arch = "aarch64")]
    {
        unsafe { neon::filter_gt_i32_neon(data, threshold, selection) }
    }
    #[cfg(not(target_arch = "aarch64"))]
    {
        scalar::filter_gt_i32_scalar(data, threshold, selection)
    }
}

/// Build a selection vector for `data[i] < threshold`.
#[inline]
pub fn filter_lt_i32(data: &[i32], threshold: i32, selection: &mut Vec<bool>) {
    #[cfg(target_arch = "aarch64")]
    {
        unsafe { neon::filter_lt_i32_neon(data, threshold, selection) }
    }
    #[cfg(not(target_arch = "aarch64"))]
    {
        scalar::filter_lt_i32_scalar(data, threshold, selection)
    }
}

/// Build a selection vector for `data[i] >= threshold`.
#[inline]
pub fn filter_gte_i32(data: &[i32], threshold: i32, selection: &mut Vec<bool>) {
    #[cfg(target_arch = "aarch64")]
    {
        unsafe { neon::filter_gte_i32_neon(data, threshold, selection) }
    }
    #[cfg(not(target_arch = "aarch64"))]
    {
        scalar::filter_gte_i32_scalar(data, threshold, selection)
    }
}

/// Build a selection vector for `data[i] <= threshold`.
#[inline]
pub fn filter_lte_i32(data: &[i32], threshold: i32, selection: &mut Vec<bool>) {
    #[cfg(target_arch = "aarch64")]
    {
        unsafe { neon::filter_lte_i32_neon(data, threshold, selection) }
    }
    #[cfg(not(target_arch = "aarch64"))]
    {
        scalar::filter_lte_i32_scalar(data, threshold, selection)
    }
}

/// Build a selection vector for `data[i] == threshold`.
#[inline]
pub fn filter_eq_i32(data: &[i32], threshold: i32, selection: &mut Vec<bool>) {
    #[cfg(target_arch = "aarch64")]
    {
        unsafe { neon::filter_eq_i32_neon(data, threshold, selection) }
    }
    #[cfg(not(target_arch = "aarch64"))]
    {
        scalar::filter_eq_i32_scalar(data, threshold, selection)
    }
}

/// Count nonzero u16 values at strided offsets in a byte slice.
/// Used by `heap_page::count_live_tuples`.
#[inline]
pub fn count_nonzero_slots(
    data: &[u8],
    start_offset: usize,
    stride_bytes: usize,
    count: usize,
) -> u16 {
    if count == 0 {
        return 0;
    }
    #[cfg(target_arch = "aarch64")]
    {
        unsafe { neon::count_nonzero_slots_neon(data, start_offset, stride_bytes, count) }
    }
    #[cfg(not(target_arch = "aarch64"))]
    {
        scalar::count_nonzero_slots_scalar(data, start_offset, stride_bytes, count)
    }
}

/// Sum a slice of f64 values.
#[inline]
pub fn sum_f64(data: &[f64]) -> f64 {
    if data.is_empty() {
        return 0.0;
    }
    #[cfg(target_arch = "aarch64")]
    {
        unsafe { neon::sum_f64_neon(data) }
    }
    #[cfg(not(target_arch = "aarch64"))]
    {
        scalar::sum_f64_scalar(data)
    }
}

/// Find the minimum of a non-empty f64 slice.
#[inline]
pub fn min_f64(data: &[f64]) -> Option<f64> {
    if data.is_empty() {
        return None;
    }
    #[cfg(target_arch = "aarch64")]
    {
        Some(unsafe { neon::min_f64_neon(data) })
    }
    #[cfg(not(target_arch = "aarch64"))]
    {
        Some(scalar::min_f64_scalar(data))
    }
}

/// Find the maximum of a non-empty f64 slice.
#[inline]
pub fn max_f64(data: &[f64]) -> Option<f64> {
    if data.is_empty() {
        return None;
    }
    #[cfg(target_arch = "aarch64")]
    {
        Some(unsafe { neon::max_f64_neon(data) })
    }
    #[cfg(not(target_arch = "aarch64"))]
    {
        Some(scalar::max_f64_scalar(data))
    }
}

// =========================================================================
// Tests
// =========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sum_i32_empty() {
        assert_eq!(sum_i32(&[]), 0);
    }

    #[test]
    fn test_sum_i32_small() {
        assert_eq!(sum_i32(&[1, 2, 3]), 6);
    }

    #[test]
    fn test_sum_i32_exact_chunk() {
        assert_eq!(sum_i32(&[10, 20, 30, 40]), 100);
    }

    #[test]
    fn test_sum_i32_with_remainder() {
        assert_eq!(sum_i32(&[1, 2, 3, 4, 5, 6, 7]), 28);
    }

    #[test]
    fn test_sum_i32_negative() {
        assert_eq!(sum_i32(&[-1, -2, 3, 4]), 4);
    }

    #[test]
    fn test_sum_i32_overflow_to_i64() {
        let data = vec![i32::MAX; 8];
        let expected = i32::MAX as i64 * 8;
        assert_eq!(sum_i32(&data), expected);
    }

    #[test]
    fn test_sum_i64_empty() {
        assert_eq!(sum_i64(&[]), 0);
    }

    #[test]
    fn test_sum_i64_values() {
        assert_eq!(sum_i64(&[100, 200, 300, 400, 500]), 1500);
    }

    #[test]
    fn test_min_i32_empty() {
        assert_eq!(min_i32(&[]), None);
    }

    #[test]
    fn test_min_i32_values() {
        assert_eq!(min_i32(&[5, 3, 8, 1, 7, 2, 9, 4, 6]), Some(1));
    }

    #[test]
    fn test_max_i32_values() {
        assert_eq!(max_i32(&[5, 3, 8, 1, 7, 2, 9, 4, 6]), Some(9));
    }

    #[test]
    fn test_filter_gt_i32() {
        let data = [1, 5, 3, 7, 2, 8, 4, 6];
        let mut sel = Vec::new();
        filter_gt_i32(&data, 4, &mut sel);
        assert_eq!(sel, vec![false, true, false, true, false, true, false, true]);
    }

    #[test]
    fn test_filter_lt_i32() {
        let data = [1, 5, 3, 7];
        let mut sel = Vec::new();
        filter_lt_i32(&data, 4, &mut sel);
        assert_eq!(sel, vec![true, false, true, false]);
    }

    #[test]
    fn test_filter_eq_i32() {
        let data = [1, 4, 3, 4, 5];
        let mut sel = Vec::new();
        filter_eq_i32(&data, 4, &mut sel);
        assert_eq!(sel, vec![false, true, false, true, false]);
    }

    #[test]
    fn test_count_nonzero_slots() {
        // Simulate a page slot array: 4-byte entries at stride 4
        // Offsets at positions 0,1 of each entry (u16 LE)
        let mut data = vec![0u8; 32];
        // Slot 0: offset = 100
        data[0] = 100;
        data[1] = 0;
        // Slot 1: offset = 0 (deleted)
        data[4] = 0;
        data[5] = 0;
        // Slot 2: offset = 200
        data[8] = 200;
        data[9] = 0;
        // Slot 3: offset = 0 (deleted)
        data[12] = 0;
        data[13] = 0;
        // Slot 4: offset = 50
        data[16] = 50;
        data[17] = 0;

        assert_eq!(count_nonzero_slots(&data, 0, 4, 5), 3);
    }

    #[test]
    fn test_sum_f64() {
        assert_eq!(sum_f64(&[1.0, 2.0, 3.0, 4.0, 5.0]), 15.0);
    }

    #[test]
    fn test_min_f64() {
        assert_eq!(min_f64(&[3.0, 1.0, 4.0, 1.5, 2.0]), Some(1.0));
    }

    #[test]
    fn test_max_f64() {
        assert_eq!(max_f64(&[3.0, 1.0, 4.0, 1.5, 2.0]), Some(4.0));
    }

    #[test]
    fn test_sum_i32_large() {
        let data: Vec<i32> = (1..=1024).collect();
        let expected: i64 = (1024 * 1025) / 2;
        assert_eq!(sum_i32(&data), expected);
    }

    #[test]
    fn test_filter_gte_i32() {
        let data = [1, 4, 3, 4, 5];
        let mut sel = Vec::new();
        filter_gte_i32(&data, 4, &mut sel);
        assert_eq!(sel, vec![false, true, false, true, true]);
    }

    #[test]
    fn test_filter_lte_i32() {
        let data = [1, 4, 3, 4, 5];
        let mut sel = Vec::new();
        filter_lte_i32(&data, 4, &mut sel);
        assert_eq!(sel, vec![true, true, true, true, false]);
    }
}
