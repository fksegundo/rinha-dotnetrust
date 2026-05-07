use std::env;
use std::ffi::CStr;
use std::fs;
use std::os::raw::{c_char, c_int, c_void};
use std::ptr;

const MAGIC_TREE: &[u8; 8] = b"RNATIDX2";
const MAGIC_IVF1: &[u8; 8] = b"RIVF1\0\0\0";
const MAGIC_IVF2: &[u8; 8] = b"RIVF2\0\0\0";
const PACKED_DIMS: usize = 16;
const DIMS: usize = 14;
const LANES: usize = 8;
const K: usize = 5; // kNN
const MAX_PROBES: usize = 128;
const MAX_PARTITIONS: usize = 64;
const TREE_STACK_CAPACITY: usize = 128;

// ─── Public index enum ────────────────────────────────────────────────────────

enum NativeIndex {
    Tree(TreeIndex),
    Ivf(IvfIndex),
}

impl NativeIndex {
    fn open(path: &str) -> Result<Self, String> {
        let bytes = fs::read(path).map_err(|e| e.to_string())?;
        if bytes.len() < 8 {
            return Err("file too short".to_string());
        }
        let magic: &[u8; 8] = bytes[..8].try_into().unwrap();
        if magic == MAGIC_IVF2 {
            Ok(NativeIndex::Ivf(IvfIndex::load(&bytes, true)?))
        } else if magic == MAGIC_IVF1 {
            Ok(NativeIndex::Ivf(IvfIndex::load(&bytes, false)?))
        } else if magic == MAGIC_TREE {
            Ok(NativeIndex::Tree(TreeIndex::load(&bytes)?))
        } else {
            Err(format!("unknown magic: {:?}", &bytes[..8]))
        }
    }

    fn predict(&self, query: &[i16; PACKED_DIMS]) -> i32 {
        match self {
            NativeIndex::Tree(t) => t.predict(query),
            NativeIndex::Ivf(i) => i.predict(query),
        }
    }
}

// ─── FFI ──────────────────────────────────────────────────────────────────────

#[unsafe(no_mangle)]
pub extern "C" fn rinha_index_open(path: *const c_char) -> *mut c_void {
    if path.is_null() {
        return ptr::null_mut();
    }
    let c_path = unsafe { CStr::from_ptr(path) };
    let Ok(path_str) = c_path.to_str() else {
        return ptr::null_mut();
    };
    match NativeIndex::open(path_str) {
        Ok(index) => Box::into_raw(Box::new(index)) as *mut c_void,
        Err(e) => {
            eprintln!("rinha_index_open error: {e}");
            ptr::null_mut()
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn rinha_predict(handle: *mut c_void, query_ptr: *const i16, len: c_int) -> c_int {
    if handle.is_null() || query_ptr.is_null() || len as usize != PACKED_DIMS {
        return -1;
    }
    let index = unsafe { &*(handle as *mut NativeIndex) };
    let query = unsafe { std::slice::from_raw_parts(query_ptr, PACKED_DIMS) };
    let mut q = [0i16; PACKED_DIMS];
    q.copy_from_slice(query);
    index.predict(&q)
}

#[unsafe(no_mangle)]
pub extern "C" fn rinha_index_close(handle: *mut c_void) {
    if handle.is_null() {
        return;
    }
    unsafe {
        drop(Box::from_raw(handle as *mut NativeIndex));
    }
}

// ─── Shared helpers ───────────────────────────────────────────────────────────

#[inline(always)]
fn insert_best(dist: i64, label: u8, best_dists: &mut [i64; K], best_labels: &mut [u8; K]) {
    if dist >= best_dists[K - 1] {
        return;
    }
    let mut pos = K - 1;
    while pos > 0 && dist < best_dists[pos - 1] {
        best_dists[pos] = best_dists[pos - 1];
        best_labels[pos] = best_labels[pos - 1];
        pos -= 1;
    }
    best_dists[pos] = dist;
    best_labels[pos] = label;
}

#[inline(always)]
fn insert_center(
    dist: i64,
    center: usize,
    dists: &mut [i64; MAX_PROBES],
    centers: &mut [usize; MAX_PROBES],
) {
    if dist >= dists[MAX_PROBES - 1] {
        return;
    }

    let mut pos = MAX_PROBES - 1;
    while pos > 0 && dist < dists[pos - 1] {
        dists[pos] = dists[pos - 1];
        centers[pos] = centers[pos - 1];
        pos -= 1;
    }

    dists[pos] = dist;
    centers[pos] = center;
}

#[inline(always)]
fn scan_block_avx2(vectors: &[i16], block_base: usize, query: &[i16; PACKED_DIMS]) -> [i64; LANES] {
    let mut block_dists = [0i64; LANES];

    #[cfg(target_arch = "x86_64")]
    unsafe {
        use std::arch::x86_64::*;
        let mut sum = _mm256_setzero_si256();
        for d in 0..DIMS {
            let q_vec = _mm_set1_epi16(query[d]);
            let v_ptr = vectors.as_ptr().add(block_base + d * LANES);
            let v_vec = _mm_loadu_si128(v_ptr as *const __m128i);
            let diff = _mm_sub_epi16(q_vec, v_vec);
            let diff32 = _mm256_cvtepi16_epi32(diff);
            let sq = _mm256_mullo_epi32(diff32, diff32);
            sum = _mm256_add_epi32(sum, sq);
        }
        let mut arr = [0i32; 8];
        _mm256_storeu_si256(arr.as_mut_ptr() as *mut __m256i, sum);
        for i in 0..LANES {
            block_dists[i] = arr[i] as i64;
        }
    }

    block_dists
}

#[inline(always)]
fn scan_block_scalar(
    vectors: &[i16],
    block_base: usize,
    query: &[i16; PACKED_DIMS],
) -> [i64; LANES] {
    let mut dists = [0i64; LANES];
    for d in 0..DIMS {
        let q = query[d] as i64;
        let base = block_base + d * LANES;
        for i in 0..LANES {
            let diff = q - vectors[base + i] as i64;
            dists[i] += diff * diff;
        }
    }
    dists
}

fn read_i32(bytes: &[u8], cursor: &mut usize) -> Result<i32, String> {
    if *cursor + 4 > bytes.len() {
        return Err("unexpected EOF (i32)".to_string());
    }
    let v = i32::from_le_bytes(bytes[*cursor..*cursor + 4].try_into().unwrap());
    *cursor += 4;
    Ok(v)
}

fn read_i16(bytes: &[u8], cursor: &mut usize) -> Result<i16, String> {
    if *cursor + 2 > bytes.len() {
        return Err("unexpected EOF (i16)".to_string());
    }
    let v = i16::from_le_bytes(bytes[*cursor..*cursor + 2].try_into().unwrap());
    *cursor += 2;
    Ok(v)
}

fn read_i16_array(bytes: &[u8], cursor: &mut usize) -> Result<[i16; PACKED_DIMS], String> {
    let mut arr = [0i16; PACKED_DIMS];
    for x in &mut arr {
        *x = read_i16(bytes, cursor)?;
    }
    Ok(arr)
}

fn env_usize(name: &str, default: usize, min: usize, max: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(default)
        .clamp(min, max)
}

// ─── IVF Index ────────────────────────────────────────────────────────────────

struct IvfIndex {
    center_count: usize,
    /// centroids transposed: [dim][center] → i16[DIMS * center_count]
    centers: Vec<i16>,
    /// block offsets per centroid (length center_count + 1)
    block_offsets: Vec<u32>,
    /// exact vector count per centroid; avoids scanning padded lanes
    cluster_counts: Vec<u32>,
    /// labels padded to block_count * LANES
    labels: Vec<u8>,
    /// SIMD blocks dimension-major: [total_blocks * DIMS * LANES]
    vectors: Vec<i16>,
    fast_probes: usize,
    full_probes: usize,
    has_avx2: bool,
}

impl IvfIndex {
    fn load(bytes: &[u8], has_cluster_counts: bool) -> Result<Self, String> {
        let mut cursor = 8usize; // skip magic

        let _scale = read_i32(bytes, &mut cursor)?;
        let packed_dims = read_i32(bytes, &mut cursor)? as usize;
        let _count = read_i32(bytes, &mut cursor)? as usize;
        let center_count = read_i32(bytes, &mut cursor)? as usize;

        if packed_dims != PACKED_DIMS {
            return Err("packed_dims mismatch".to_string());
        }

        // centroids transposed: i16[DIMS * center_count]
        let centers_len = DIMS * center_count;
        let centers_bytes = centers_len * 2;
        if cursor + centers_bytes > bytes.len() {
            return Err("truncated centers".to_string());
        }
        let mut centers = vec![0i16; centers_len];
        for x in &mut centers {
            *x = read_i16(bytes, &mut cursor)?;
        }

        // block offsets: u32[center_count + 1]
        let offsets_len = center_count + 1;
        let mut block_offsets = Vec::with_capacity(offsets_len);
        for _ in 0..offsets_len {
            block_offsets.push(read_i32(bytes, &mut cursor)? as u32);
        }
        let total_blocks = *block_offsets.last().unwrap() as usize;

        let cluster_counts = if has_cluster_counts {
            let mut counts = Vec::with_capacity(center_count);
            for _ in 0..center_count {
                counts.push(read_i32(bytes, &mut cursor)? as u32);
            }
            counts
        } else {
            let mut counts = Vec::with_capacity(center_count);
            for c in 0..center_count {
                counts.push((block_offsets[c + 1] - block_offsets[c]) * LANES as u32);
            }
            counts
        };

        // labels: u8[total_blocks * LANES]
        let labels_len = total_blocks * LANES;
        if cursor + labels_len > bytes.len() {
            return Err("truncated labels".to_string());
        }
        let labels = bytes[cursor..cursor + labels_len].to_vec();
        cursor += labels_len;

        // vectors: i16[total_blocks * DIMS * LANES]
        let vectors_len = total_blocks * DIMS * LANES;
        let vectors_bytes = vectors_len * 2;
        if cursor + vectors_bytes > bytes.len() {
            return Err("truncated vectors".to_string());
        }
        let mut vectors = vec![0i16; vectors_len];
        for x in &mut vectors {
            *x = read_i16(bytes, &mut cursor)?;
        }

        let has_avx2 = cfg!(target_arch = "x86_64") && std::arch::is_x86_feature_detected!("avx2");
        let full_probes = env_usize(
            "RINHA_NATIVE_FULL_PROBES",
            64,
            1,
            center_count.min(MAX_PROBES),
        );
        let fast_probes = env_usize("RINHA_NATIVE_FAST_PROBES", 16, 1, full_probes);

        eprintln!(
            "[RIVF] loaded: {} centers, {} total blocks, {} vectors, probes={}/{}, avx2={}",
            center_count,
            total_blocks,
            cluster_counts.iter().map(|&v| v as usize).sum::<usize>(),
            fast_probes,
            full_probes,
            has_avx2
        );

        Ok(Self {
            center_count,
            centers,
            block_offsets,
            cluster_counts,
            labels,
            vectors,
            fast_probes,
            full_probes,
            has_avx2,
        })
    }

    fn center_dist(&self, query: &[i16; PACKED_DIMS], c: usize) -> i64 {
        let mut dist = 0i64;
        for d in 0..DIMS {
            let diff = query[d] as i64 - self.centers[d * self.center_count + c] as i64;
            dist += diff * diff;
        }
        dist
    }

    fn scan_probes(
        &self,
        query: &[i16; PACKED_DIMS],
        probe_centers: &[usize],
        best_dists: &mut [i64; K],
        best_labels: &mut [u8; K],
    ) {
        for &c in probe_centers {
            let start = self.block_offsets[c] as usize;
            let count = self.cluster_counts[c] as usize;
            let blocks = (count + LANES - 1) / LANES;

            for b in 0..blocks {
                let block_idx = start + b;
                let lanes = if b + 1 == blocks {
                    count - b * LANES
                } else {
                    LANES
                };
                if lanes == 0 {
                    break;
                }

                let block_base = block_idx * DIMS * LANES;
                let dists = if self.has_avx2 {
                    scan_block_avx2(&self.vectors, block_base, query)
                } else {
                    scan_block_scalar(&self.vectors, block_base, query)
                };
                let labels_base = block_idx * LANES;
                for i in 0..lanes {
                    insert_best(
                        dists[i],
                        self.labels[labels_base + i],
                        best_dists,
                        best_labels,
                    );
                }
            }
        }
    }

    fn predict(&self, query: &[i16; PACKED_DIMS]) -> i32 {
        let mut center_dists = [i64::MAX; MAX_PROBES];
        let mut center_ids = [0usize; MAX_PROBES];
        for c in 0..self.center_count {
            insert_center(
                self.center_dist(query, c),
                c,
                &mut center_dists,
                &mut center_ids,
            );
        }

        let mut best_dists = [i64::MAX; K];
        let mut best_labels = [0u8; K];

        self.scan_probes(
            query,
            &center_ids[..self.fast_probes],
            &mut best_dists,
            &mut best_labels,
        );

        let fraud_count: i32 = best_labels.iter().map(|&l| l as i32).sum();
        if fraud_count > 0 && fraud_count < 5 {
            self.scan_probes(
                query,
                &center_ids[self.fast_probes..self.full_probes],
                &mut best_dists,
                &mut best_labels,
            );
        }

        best_labels.iter().map(|&l| l as i32).sum()
    }
}

// ─── Tree Index (RNATIDX2 - kept as fallback) ─────────────────────────────────

struct TreeIndex {
    partitions: Vec<Partition>,
    nodes: Vec<Node>,
    vectors: Vec<i16>,
    labels: Vec<u8>,
    has_avx2: bool,
}

#[derive(Clone)]
struct Partition {
    root: usize,
    min: [i16; PACKED_DIMS],
    max: [i16; PACKED_DIMS],
}

#[derive(Clone)]
struct Node {
    left: i32,
    right: i32,
    start: usize,
    len: usize,
    min: [i16; PACKED_DIMS],
    max: [i16; PACKED_DIMS],
}

impl TreeIndex {
    fn load(bytes: &[u8]) -> Result<Self, String> {
        let mut cursor = 8usize; // skip magic

        let _scale = read_i32(bytes, &mut cursor)?;
        let packed_dims = read_i32(bytes, &mut cursor)? as usize;
        let _count = read_i32(bytes, &mut cursor)?;
        let _leaf_size = read_i32(bytes, &mut cursor)?;
        let partition_count = read_i32(bytes, &mut cursor)? as usize;
        let node_count = read_i32(bytes, &mut cursor)? as usize;
        let total_blocks = read_i32(bytes, &mut cursor)? as usize;

        if packed_dims != PACKED_DIMS {
            return Err("invalid packed dimensions".to_string());
        }

        let mut partitions = Vec::with_capacity(partition_count);
        for _ in 0..partition_count {
            let _key = read_i32(bytes, &mut cursor)?;
            let root = read_i32(bytes, &mut cursor)? as usize;
            let _start = read_i32(bytes, &mut cursor)?;
            let _len = read_i32(bytes, &mut cursor)?;
            let min = read_i16_array(bytes, &mut cursor)?;
            let max = read_i16_array(bytes, &mut cursor)?;
            partitions.push(Partition { root, min, max });
        }

        let mut nodes = Vec::with_capacity(node_count);
        for _ in 0..node_count {
            let left = read_i32(bytes, &mut cursor)?;
            let right = read_i32(bytes, &mut cursor)?;
            let start = read_i32(bytes, &mut cursor)? as usize;
            let len = read_i32(bytes, &mut cursor)? as usize;
            let min = read_i16_array(bytes, &mut cursor)?;
            let max = read_i16_array(bytes, &mut cursor)?;
            nodes.push(Node {
                left,
                right,
                start,
                len,
                min,
                max,
            });
        }

        let vectors_len = total_blocks * DIMS * LANES;
        let mut vectors = vec![0i16; vectors_len];
        for x in &mut vectors {
            *x = read_i16(bytes, &mut cursor)?;
        }

        let labels_len = total_blocks * LANES;
        if cursor + labels_len > bytes.len() {
            return Err("truncated labels".to_string());
        }
        let labels = bytes[cursor..cursor + labels_len].to_vec();

        let has_avx2 = cfg!(target_arch = "x86_64") && std::arch::is_x86_feature_detected!("avx2");

        eprintln!(
            "[RNATIDX2] loaded: {} partitions, {} nodes, {} blocks, avx2={}",
            partition_count, node_count, total_blocks, has_avx2
        );

        Ok(Self {
            partitions,
            nodes,
            vectors,
            labels,
            has_avx2,
        })
    }

    fn predict(&self, query: &[i16; PACKED_DIMS]) -> i32 {
        let mut best_dists = [i64::MAX; K];
        let mut best_labels = [0u8; K];

        let mut partition_bounds = [i64::MAX; MAX_PARTITIONS];
        let mut partition_ids = [0usize; MAX_PARTITIONS];
        let mut partition_len = 0usize;

        for (idx, partition) in self.partitions.iter().enumerate() {
            let bound = lower_bound_box(query, &partition.min, &partition.max);
            let mut pos = partition_len;
            while pos > 0 && bound < partition_bounds[pos - 1] {
                partition_bounds[pos] = partition_bounds[pos - 1];
                partition_ids[pos] = partition_ids[pos - 1];
                pos -= 1;
            }

            partition_bounds[pos] = bound;
            partition_ids[pos] = idx;
            partition_len += 1;
        }

        for i in 0..partition_len {
            let bound = partition_bounds[i];
            if bound >= best_dists[K - 1] {
                break;
            }

            self.search_node_iterative(
                self.partitions[partition_ids[i]].root,
                bound,
                query,
                &mut best_dists,
                &mut best_labels,
            );
        }

        best_labels.iter().map(|&l| l as i32).sum()
    }

    fn search_node_iterative(
        &self,
        root: usize,
        root_bound: i64,
        query: &[i16; PACKED_DIMS],
        best_dists: &mut [i64; K],
        best_labels: &mut [u8; K],
    ) {
        let mut stack_nodes = [0usize; TREE_STACK_CAPACITY];
        let mut stack_bounds = [0i64; TREE_STACK_CAPACITY];
        let mut stack_len = 0usize;

        let mut current = root;
        let mut current_bound = root_bound;

        loop {
            if current_bound < best_dists[K - 1] {
                let node = &self.nodes[current];
                if node.left < 0 || node.right < 0 {
                    self.scan_leaf(node, query, best_dists, best_labels);
                } else {
                    let l = node.left as usize;
                    let r = node.right as usize;

                    #[cfg(target_arch = "x86_64")]
                    unsafe {
                        use std::arch::x86_64::*;
                        _mm_prefetch((&self.nodes[r]) as *const _ as *const i8, _MM_HINT_T0);
                    }

                    let lb = lower_bound_box(query, &self.nodes[l].min, &self.nodes[l].max);
                    let rb = lower_bound_box(query, &self.nodes[r].min, &self.nodes[r].max);

                    let (near_idx, near_bound, far_idx, far_bound) = if lb <= rb {
                        (l, lb, r, rb)
                    } else {
                        (r, rb, l, lb)
                    };

                    if far_bound < best_dists[K - 1] && stack_len < TREE_STACK_CAPACITY {
                        stack_nodes[stack_len] = far_idx;
                        stack_bounds[stack_len] = far_bound;
                        stack_len += 1;
                    }

                    if near_bound < best_dists[K - 1] {
                        current = near_idx;
                        current_bound = near_bound;
                        continue;
                    }
                }
            }

            if stack_len == 0 {
                break;
            }

            stack_len -= 1;
            current = stack_nodes[stack_len];
            current_bound = stack_bounds[stack_len];
        }
    }

    fn scan_leaf(
        &self,
        node: &Node,
        query: &[i16; PACKED_DIMS],
        best_dists: &mut [i64; K],
        best_labels: &mut [u8; K],
    ) {
        let start_block = node.start;
        let blocks = (node.len + LANES - 1) / LANES;

        for b in 0..blocks {
            let block_idx = start_block + b;
            let block_base = block_idx * DIMS * LANES;

            // Prefetch next block's vectors while processing this one
            #[cfg(target_arch = "x86_64")]
            if b + 1 < blocks {
                unsafe {
                    use std::arch::x86_64::*;
                    let next_base = (start_block + b + 1) * DIMS * LANES;
                    _mm_prefetch(
                        self.vectors.as_ptr().add(next_base) as *const i8,
                        _MM_HINT_T0,
                    );
                }
            }

            let dists = if self.has_avx2 {
                scan_block_avx2(&self.vectors, block_base, query)
            } else {
                scan_block_scalar(&self.vectors, block_base, query)
            };
            let labels_base = block_idx * LANES;
            // Process all LANES unconditionally — padding lanes have dist computed
            // against zero vectors which is typically large and won't displace real results.
            // The last block may have padding entries with label=0 (legit) — safe since
            // insert_best only inserts if dist < best_dists[4].
            for i in 0..LANES {
                insert_best(
                    dists[i],
                    self.labels[labels_base + i],
                    best_dists,
                    best_labels,
                );
            }
        }
    }
}

/// Compute L2 lower bound from query to bounding box.
/// Only iterates DIMS=14 real dimensions — the last 2 slots in PACKED_DIMS=16
/// are always zero-padded and contribute nothing to the distance.
#[inline(always)]
fn lower_bound_box(
    query: &[i16; PACKED_DIMS],
    min: &[i16; PACKED_DIMS],
    max: &[i16; PACKED_DIMS],
) -> i64 {
    let mut sum = 0i64;
    for d in 0..DIMS {
        let q = query[d] as i64;
        let lo = min[d] as i64;
        let hi = max[d] as i64;
        let diff = if q < lo {
            lo - q
        } else if q > hi {
            q - hi
        } else {
            0
        };
        sum += diff * diff;
    }
    sum
}
