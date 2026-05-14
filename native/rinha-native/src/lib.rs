use std::ffi::CStr;
use std::fs::File;
use std::mem;
use std::os::fd::AsRawFd;
use std::os::raw::{c_char, c_int, c_void};
use std::ptr;
use std::slice;

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
    _mapping: MmapRegion,
    partitions: Vec<Partition>,
    nodes: Vec<Node>,
    vectors: *const i16,
    vectors_len: usize,
    labels: *const u8,
    labels_len: usize,
    has_avx2: bool,
    max_leaf_visits: usize,
}

struct MmapRegion {
    ptr: *mut u8,
    len: usize,
}

unsafe impl Send for MmapRegion {}
unsafe impl Sync for MmapRegion {}
unsafe impl Send for NativeIndex {}
unsafe impl Sync for NativeIndex {}

impl MmapRegion {
    pub fn open(path: &str) -> Result<Self, String> {
        let file = File::open(path).map_err(|e| e.to_string())?;
        let len = file.metadata().map_err(|e| e.to_string())?.len() as usize;
        if len == 0 {
            return Err("empty file".to_string());
        }

        #[cfg(target_os = "linux")]
        unsafe {
            let ptr = libc::mmap(
                ptr::null_mut(),
                len,
                libc::PROT_READ,
                libc::MAP_PRIVATE,
                file.as_raw_fd(),
                0,
            );
            if ptr == libc::MAP_FAILED {
                return Err(std::io::Error::last_os_error().to_string());
            }

            Ok(Self {
                ptr: ptr.cast::<u8>(),
                len,
            })
        }

        #[cfg(not(target_os = "linux"))]
        {
            let _ = file;
            Err("mmap index loading requires linux".to_string())
        }
    }

    fn as_slice(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.ptr.cast_const(), self.len) }
    }
}

impl Drop for MmapRegion {
    fn drop(&mut self) {
        #[cfg(target_os = "linux")]
        unsafe {
            libc::munmap(self.ptr.cast::<libc::c_void>(), self.len);
        }
    }
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
    pub fn open(path: &str) -> Result<Self, String> {
        let mapping = MmapRegion::open(path)?;
        let bytes = mapping.as_slice();
        if bytes.len() < 8 {
            return Err("file too short".to_string());
        }
        let magic: &[u8; 8] = bytes[..8].try_into().unwrap();
        if magic != MAGIC_TREE {
            return Err(format!("unknown or unsupported magic: {:?}", magic));
        }
        NativeIndex::load(mapping)
    }

    fn load(mapping: MmapRegion) -> Result<Self, String> {
        let bytes = mapping.as_slice();
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
        let vectors_bytes = vectors_len * mem::size_of::<i16>();
        if cursor % mem::align_of::<i16>() != 0 {
            return Err("unaligned vectors section".to_string());
        }
        if cursor + vectors_bytes > bytes.len() {
            return Err("truncated vectors".to_string());
        }
        let vectors = unsafe { bytes.as_ptr().add(cursor).cast::<i16>() };
        cursor += vectors_bytes;

        let labels_len = total_blocks * LANES;
        if cursor + labels_len > bytes.len() {
            return Err("truncated labels".to_string());
        }
        let labels = unsafe { bytes.as_ptr().add(cursor) };

        let has_avx2 = cfg!(target_arch = "x86_64") && std::arch::is_x86_feature_detected!("avx2");
        let max_leaf_visits = std::env::var("RINHA_MAX_LEAF_VISITS")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(0);

        eprintln!(
            "[RNATIDX2] mmap loaded: {} partitions, {} nodes, {} blocks, avx2={}, max_leaf_visits={}",
            partition_count, node_count, total_blocks, has_avx2, max_leaf_visits
        );

        let index = Self {
            _mapping: mapping,
            partitions,
            nodes,
            vectors,
            vectors_len,
            labels,
            labels_len,
            has_avx2,
            max_leaf_visits,
        };
        index.advise_hugepages();
        Ok(index)
    }

    pub fn predict(&self, query: &[i16; PACKED_DIMS]) -> i32 {
        let mut best_dists = [i64::MAX; K];
        let mut best_labels = [0u8; K];
        let mut leaf_visits = 0usize;

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
                &mut leaf_visits,
            );

            if self.max_leaf_visits > 0 && leaf_visits >= self.max_leaf_visits {
                break;
            }
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
        leaf_visits: &mut usize,
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
                    if self.max_leaf_visits > 0 && *leaf_visits >= self.max_leaf_visits {
                        break;
                    }
                    self.scan_leaf(node, query, best_dists, best_labels);
                    *leaf_visits += 1;
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

            if self.max_leaf_visits > 0 && *leaf_visits >= self.max_leaf_visits {
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
        let vectors = self.vectors();
        let labels = self.labels();

        for b in 0..blocks {
            let block_idx = start_block + b;
            let block_base = block_idx * DIMS * LANES;

            #[cfg(target_arch = "x86_64")]
            if b + 1 < blocks {
                unsafe {
                    use std::arch::x86_64::*;
                    let next_base = (start_block + b + 1) * DIMS * LANES;
                    _mm_prefetch(
                        self.vectors.add(next_base) as *const i8,
                        _MM_HINT_T0,
                    );
                }
            }

            let dists = if self.has_avx2 {
                scan_block_avx2(vectors, block_base, query)
            } else {
                scan_block_scalar(vectors, block_base, query)
            };
            let labels_base = block_idx * LANES;
            let lane_count = (node.len - b * LANES).min(LANES);
            for i in 0..lane_count {
                insert_best(
                    dists[i],
                    labels[labels_base + i],
                    best_dists,
                    best_labels,
                );
            }
        }
    }

    fn vectors(&self) -> &[i16] {
        unsafe { slice::from_raw_parts(self.vectors, self.vectors_len) }
    }

    fn labels(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.labels, self.labels_len) }
    }

    fn advise_hugepages(&self) {
        #[cfg(target_os = "linux")]
        unsafe {
            let vptr = self.vectors as *mut libc::c_void;
            let vlen = self.vectors_len * mem::size_of::<i16>();
            libc::madvise(vptr, vlen, libc::MADV_HUGEPAGE);

            let lptr = self.labels as *mut libc::c_void;
            let llen = self.labels_len;
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
        let mut sum32 = _mm256_setzero_si256();
        for d in 0..DIMS {
            let q_vec = _mm_set1_epi16(query[d]);
            let v_ptr = vectors.as_ptr().add(block_base + d * LANES);
            let v_vec = _mm_loadu_si128(v_ptr as *const __m128i);
            let diff = _mm_sub_epi16(q_vec, v_vec);
            let diff32 = _mm256_cvtepi16_epi32(diff);
            let sq = _mm256_mullo_epi32(diff32, diff32);
            sum32 = _mm256_add_epi32(sum32, sq);

            // Flush to i64 every 4 dims to avoid i32 overflow:
            // 4 * 268_435_456 = 1_073_741_824 < i32::MAX.
            if (d + 1) % 4 == 0 {
                let mut arr = [0i32; LANES];
                _mm256_storeu_si256(arr.as_mut_ptr() as *mut __m256i, sum32);
                for i in 0..LANES {
                    block_dists[i] += arr[i] as i64;
                }
                sum32 = _mm256_setzero_si256();
            }
        }
        // Flush remainder
        let mut arr = [0i32; LANES];
        _mm256_storeu_si256(arr.as_mut_ptr() as *mut __m256i, sum32);
        for i in 0..LANES {
            block_dists[i] += arr[i] as i64;
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
// Usa saturating subtract para clamp-to-zero + madd para quadrado.
// Acumulação final em i64 para evitar overflow da soma total em i32.
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
        let below = _mm256_max_epi16(_mm256_sub_epi16(mn, q), zero);
        let above = _mm256_max_epi16(_mm256_sub_epi16(q, mx), zero);
        let diff = _mm256_max_epi16(below, above);

        let sq = _mm256_madd_epi16(diff, diff);

        // Accumulate 8 i32 values into i64 to avoid overflow
        let mut arr = [0i32; 8];
        _mm256_storeu_si256(arr.as_mut_ptr() as *mut __m256i, sq);
        arr.iter().map(|&x| x as i64).sum()
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
