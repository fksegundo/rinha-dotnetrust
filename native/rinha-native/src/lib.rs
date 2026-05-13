use std::ffi::CStr;
use std::fs;
use std::os::raw::{c_char, c_int, c_void};
use std::ptr;

#[cfg(target_os = "linux")]
extern crate libc;

const MAGIC_TREE: &[u8; 8] = b"RNATIDX2";
const PACKED_DIMS: usize = 16;
const DIMS: usize = 14;
const LANES: usize = 8;
const K: usize = 5; // kNN
const MAX_PARTITIONS: usize = 256;
const TREE_STACK_CAPACITY: usize = 128;

// ─── Native Index (Tree / RNATIDX2) ──────────────────────────────────────────

pub struct NativeIndex {
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

impl NativeIndex {
    fn open(path: &str) -> Result<Self, String> {
        let bytes = fs::read(path).map_err(|e| e.to_string())?;
        if bytes.len() < 8 {
            return Err("file too short".to_string());
        }
        let magic: &[u8; 8] = bytes[..8].try_into().unwrap();
        if magic != MAGIC_TREE {
            return Err(format!("unknown or unsupported magic: {:?}", magic));
        }
        NativeIndex::load(&bytes)
    }

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

        let index = Self { partitions, nodes, vectors, labels, has_avx2 };
        index.advise_hugepages();
        Ok(index)
    }

    fn predict(&self, query: &[i16; PACKED_DIMS]) -> i32 {
        let mut best_dists = [i64::MAX; K];
        let mut best_labels = [0u8; K];

        let mut partition_entries = [(0i64, 0usize); MAX_PARTITIONS];
        let mut partition_len = 0usize;

        for (idx, partition) in self.partitions.iter().enumerate() {
            let bound = lower_bound_box(query, &partition.min, &partition.max, self.has_avx2);
            partition_entries[partition_len] = (bound, idx);
            partition_len += 1;
        }

        partition_entries[..partition_len].sort_unstable_by_key(|&(bound, _)| bound);

        for i in 0..partition_len {
            let (bound, idx) = partition_entries[i];
            if bound >= best_dists[K - 1] {
                break;
            }

            self.search_node_iterative(
                self.partitions[idx].root,
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

                    let lb = lower_bound_box(query, &self.nodes[l].min, &self.nodes[l].max, self.has_avx2);
                    let rb = lower_bound_box(query, &self.nodes[r].min, &self.nodes[r].max, self.has_avx2);

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
            let lane_count = (node.len - b * LANES).min(LANES);
            for i in 0..lane_count {
                insert_best(
                    dists[i],
                    self.labels[labels_base + i],
                    best_dists,
                    best_labels,
                );
            }
        }
    }

    fn advise_hugepages(&self) {
        #[cfg(target_os = "linux")]
        unsafe {
            let vptr = self.vectors.as_ptr() as *mut libc::c_void;
            let vlen = self.vectors.len() * std::mem::size_of::<i16>();
            libc::madvise(vptr, vlen, libc::MADV_HUGEPAGE);

            let lptr = self.labels.as_ptr() as *mut libc::c_void;
            let llen = self.labels.len();
            libc::madvise(lptr, llen, libc::MADV_HUGEPAGE);
        }
    }
}

// ─── FFI Exports ─────────────────────────────────────────────────────────────

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
    let query = unsafe { &*(query_ptr as *const [i16; PACKED_DIMS]) };
    index.predict(query)
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

// ─── Private Helpers ─────────────────────────────────────────────────────────

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

#[inline(always)]
fn lower_bound_box(
    query: &[i16; PACKED_DIMS],
    min: &[i16; PACKED_DIMS],
    max: &[i16; PACKED_DIMS],
    has_avx2: bool,
) -> i64 {
    #[cfg(target_arch = "x86_64")]
    if has_avx2 {
        return unsafe { lower_bound_box_avx2(query, min, max) };
    }
    lower_bound_box_scalar(query, min, max)
}

// Processa todas as 16 dimensões (14 reais + 2 padding=0) em 1 passada AVX2.
// Usa saturating subtract para clamp-to-zero + madd para quadrado e redução por pares.
// Diferença máx por dim: 8192-(-8192)=16384 → cabe em i16; soma de 2 quadrados ≤ 536M < i32::MAX.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn lower_bound_box_avx2(
    query: &[i16; PACKED_DIMS],
    min: &[i16; PACKED_DIMS],
    max: &[i16; PACKED_DIMS],
) -> i64 {
    use std::arch::x86_64::*;

    // SAFETY: guarded by #[target_feature(enable = "avx2")] e verificação em runtime no chamador
    unsafe {
        let q  = _mm256_loadu_si256(query.as_ptr() as *const __m256i);
        let mn = _mm256_loadu_si256(min.as_ptr()   as *const __m256i);
        let mx = _mm256_loadu_si256(max.as_ptr()   as *const __m256i);

        let zero = _mm256_setzero_si256();
        // distância para baixo do box: max(0, min - query)
        let below = _mm256_max_epi16(_mm256_sub_epi16(mn, q), zero);
        // distância para cima do box: max(0, query - max)
        let above = _mm256_max_epi16(_mm256_sub_epi16(q, mx), zero);
        // Por dimensão, só um dos lados é não-zero
        let diff = _mm256_max_epi16(below, above);

        // madd: diff[i]²+diff[i+1]² → 8 valores i32 (soma por pares)
        let sq = _mm256_madd_epi16(diff, diff);

        // Redução horizontal: 8 → 4 → 2 → 1 i32
        let lo = _mm256_castsi256_si128(sq);
        let hi = _mm256_extracti128_si256(sq, 1);
        let s4 = _mm_add_epi32(lo, hi);
        let s2 = _mm_hadd_epi32(s4, s4);
        let s1 = _mm_hadd_epi32(s2, s2);
        _mm_cvtsi128_si32(s1) as i64
    }
}

#[inline(always)]
fn lower_bound_box_scalar(
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
