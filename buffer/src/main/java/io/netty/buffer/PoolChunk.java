/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.buffer;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.PriorityQueue;

/**
 * Description of algorithm for PageRun/PoolSubpage allocation from PoolChunk
 *
 * Notation: The following terms are important to understand the code
 * > page  - a page is the smallest unit of memory chunk that can be allocated
 * > run   - a run is a collection of pages
 * > chunk - a chunk is a collection of runs
 * > in this code chunkSize = maxPages * pageSize
 *
 * To begin we allocate a byte array of size = chunkSize
 * Whenever a ByteBuf of given size needs to be created we search for the first position
 * in the byte array that has enough empty space to accommodate the requested size and
 * return a (long) handle that encodes this offset information, (this memory segment is then
 * marked as reserved so it is always used by exactly one ByteBuf and no more)
 *
 * For simplicity all sizes are normalized according to {@link PoolArena#size2SizeIdx(int)} method.
 * This ensures that when we request for memory segments of size > pageSize the normalizedCapacity
 * equals the next nearest size in {@link SizeClasses}.
 *
 *
 *  A chunk has the following layout:
 *
 *     /-----------------\
 *     | run             |
 *     |                 |
 *     |                 |
 *     |-----------------|
 *     | run             |
 *     |                 |
 *     |-----------------|
 *     | unalloctated    |
 *     | (freed)         |
 *     |                 |
 *     |-----------------|
 *     | subpage         |
 *     |-----------------|
 *     | unallocated     |
 *     | (freed)         |
 *     | ...             |
 *     | ...             |
 *     | ...             |
 *     |                 |
 *     |                 |
 *     |                 |
 *     \-----------------/
 *
 *
 * handle:
 * -------
 * a handle is a long number, the bit layout of a run looks like:
 *
 * oooooooo ooooooos ssssssss ssssssue bbbbbbbb bbbbbbbb bbbbbbbb bbbbbbbb
 *
 * o: runOffset (page offset in the chunk), 15bit
 * s: size (number of pages) of this run, 15bit
 * u: isUsed?, 1bit
 * e: isSubpage?, 1bit
 * b: bitmapIdx of subpage, zero if it's not subpage, 32bit
 *
 * runsAvailMap:
 * ------
 * a map which manages all runs (used and not in used).
 * For each run, the first runOffset and last runOffset are stored in runsAvailMap.
 * key: runOffset
 * value: handle
 *
 * runsAvail:
 * ----------
 * an array of {@link PriorityQueue}.
 * Each queue manages same size of runs.
 * Runs are sorted by offset, so that we always allocate runs with smaller offset.
 *
 *
 * Algorithm:
 * ----------
 *
 *   As we allocate runs, we update values stored in runsAvailMap and runsAvail so that the property is maintained.
 *
 * Initialization -
 *  In the beginning we store the initial run which is the whole chunk.
 *  The initial run:
 *  runOffset = 0
 *  size = chunkSize
 *  isUsed = no
 *  isSubpage = no
 *  bitmapIdx = 0
 *
 *
 * Algorithm: [allocateRun(size)]
 * ----------
 * 1) find the first avail run using in runsAvails according to size
 * 2) if pages of run is larger than request pages then split it, and save the tailing run
 *    for later using
 *
 * Algorithm: [allocateSubpage(size)]
 * ----------
 * 1) find a not full subpage according to size.
 *    if it already exists just return, otherwise allocate a new PoolSubpage and call init()
 *    note that this subpage object is added to subpagesPool in the PoolArena when we init() it
 * 2) call subpage.allocate()
 *
 * Algorithm: [free(handle, length, nioBuffer)]
 * ----------
 * 1) if it is a subpage, return the slab back into this subpage
 * 2) if the subpage is not used or it is a run, then start free this run
 * 3) merge continuous avail runs
 * 4) save the merged run
 *
 */
final class PoolChunk<T> implements PoolChunkMetric {
    private static final int SIZE_BIT_LENGTH = 15;
    private static final int INUSED_BIT_LENGTH = 1;
    private static final int SUBPAGE_BIT_LENGTH = 1;
    private static final int BITMAP_IDX_BIT_LENGTH = 32;

    static final int IS_SUBPAGE_SHIFT = BITMAP_IDX_BIT_LENGTH;
    static final int IS_USED_SHIFT = SUBPAGE_BIT_LENGTH + IS_SUBPAGE_SHIFT;
    static final int SIZE_SHIFT = INUSED_BIT_LENGTH + IS_USED_SHIFT;
    static final int RUN_OFFSET_SHIFT = SIZE_BIT_LENGTH + SIZE_SHIFT;

    final PoolArena<T> arena;
    final Object base;
    final T memory;
    final boolean unpooled;

    /**
     * store the first page and last page of each avail run
     */
    private final LongLongHashMap runsAvailMap;

    /**
     * manage all avail runs
     */
    private final LongPriorityQueue[] runsAvail;

    /**
     * manage all subpages in this chunk
     */
    private final PoolSubpage<T>[] subpages;// 数量就是page数

    private final int pageSize;
    private final int pageShifts;
    private final int chunkSize;

    // Use as cache for ByteBuffer created from the memory. These are just duplicates and so are only a container
    // around the memory itself. These are often needed for operations within the Pooled*ByteBuf and so
    // may produce extra GC, which can be greatly reduced by caching the duplicates.
    //
    // This may be null if the PoolChunk is unpooled as pooling the ByteBuffer instances does not make any sense here.
    private final Deque<ByteBuffer> cachedNioBuffers;

    int freeBytes;

    PoolChunkList<T> parent;
    PoolChunk<T> prev;
    PoolChunk<T> next;

    // TODO: Test if adding padding helps under contention
    //private long pad0, pad1, pad2, pad3, pad4, pad5, pad6, pad7;

    @SuppressWarnings("unchecked")
    PoolChunk(PoolArena<T> arena, Object base, T memory, int pageSize, int pageShifts, int chunkSize, int maxPageIdx) {
        unpooled = false;
        this.arena = arena;
        this.base = base;
        this.memory = memory;
        this.pageSize = pageSize;
        this.pageShifts = pageShifts;
        this.chunkSize = chunkSize;
        freeBytes = chunkSize;

        // 每个PageIdx（page段）都创建队列对象
        runsAvail = newRunsAvailqueueArray(maxPageIdx);
        runsAvailMap = new LongLongHashMap(-1);

        // subpages 即PoolSubpage，数量就是page数
        // 就是chunk 可以分多少个page：2000个（16m/8kb）
        subpages = new PoolSubpage[chunkSize >> pageShifts];

        //insert initial run, offset = 0, pages = chunkSize / pageSize
        int pages = chunkSize >> pageShifts;
        // 其实就是handle的标记，前13位是0，跟着15位是page个数，初始第一个后34位都是0
        long initHandle = (long) pages << SIZE_SHIFT;
        insertAvailRun(0, pages, initHandle);// 初始化page的queue，放入map中映射 0到initHandle

        cachedNioBuffers = new ArrayDeque<ByteBuffer>(8);
    }

    /** Creates a special chunk that is not pooled. */
    PoolChunk(PoolArena<T> arena, Object base, T memory, int size) {
        unpooled = true;
        this.arena = arena;
        this.base = base;
        this.memory = memory;// ByteBuffer对象
        pageSize = 0;
        pageShifts = 0;
        runsAvailMap = null;
        runsAvail = null;
        subpages = null;
        chunkSize = size;
        cachedNioBuffers = null;
    }

    private static LongPriorityQueue[] newRunsAvailqueueArray(int size) {
        LongPriorityQueue[] queueArray = new LongPriorityQueue[size];
        for (int i = 0; i < queueArray.length; i++) {
            queueArray[i] = new LongPriorityQueue();
        }
        return queueArray;
    }

    private void insertAvailRun(int runOffset, int pages, long handle) {
        // 也是根据page大小分段得到序号 -》此时最后是最后1个page段
        int pageIdxFloor = arena.pages2pageIdxFloor(pages);

        // 把handle放入 最后1个page段的queue
        LongPriorityQueue queue = runsAvail[pageIdxFloor];
        queue.offer(handle);

        // 初始化这个handle
        //insert first page of run
        insertAvailRun0(runOffset, handle);// 就放到map，做runOffset到handle的映射
        if (pages > 1) {// page超过1个
            //insert last page of run
            insertAvailRun0(lastPage(runOffset, pages), handle);// 最后1个也映射到handle
        }
    }

    private void insertAvailRun0(int runOffset, long handle) {
        long pre = runsAvailMap.put(runOffset, handle);
        assert pre == -1;
    }

    private void removeAvailRun(long handle) {
        int pageIdxFloor = arena.pages2pageIdxFloor(runPages(handle));
        LongPriorityQueue queue = runsAvail[pageIdxFloor];
        removeAvailRun(queue, handle);
    }

    private void removeAvailRun(LongPriorityQueue queue, long handle) {
        queue.remove(handle);// 队列移除

        int runOffset = runOffset(handle);// 前13位
        int pages = runPages(handle);// 前13位的开始15位
        //remove first page of run
        runsAvailMap.remove(runOffset);// 移除runoffset映射，应该在队列中才会在map
        if (pages > 1) {
            //remove last page of run红
            runsAvailMap.remove(lastPage(runOffset, pages));
        }
    }

    private static int lastPage(int runOffset, int pages) {
        return runOffset + pages - 1;
    }

    private long getAvailRunByOffset(int runOffset) {
        return runsAvailMap.get(runOffset);
    }

    @Override
    public int usage() {
        final int freeBytes;
        synchronized (arena) {
            freeBytes = this.freeBytes;
        }
        return usage(freeBytes);
    }

    private int usage(int freeBytes) {
        if (freeBytes == 0) {
            return 100;
        }

        int freePercentage = (int) (freeBytes * 100L / chunkSize);
        if (freePercentage == 0) {
            return 99;
        }
        return 100 - freePercentage;
    }

    boolean allocate(PooledByteBuf<T> buf, int reqCapacity, int sizeIdx, PoolThreadCache cache) {
        final long handle;
        // 申请大小属于small的
        if (sizeIdx <= arena.smallMaxSizeIdx) {
            // small -》在当前chunk申请需求大小对齐后的连续subPage（可能占用多个page）
            handle = allocateSubpage(sizeIdx);
            // 负数-1就是失败了
            if (handle < 0) {
                return false;
            }
            assert isSubpage(handle);
        } else {
            // normal page 处理(small的还会在这个继续生成subpage用于复用)
            // runSize must be multiple of pageSize
            int runSize = arena.sizeIdx2size(sizeIdx);

            // 通用分配
            handle = allocateRun(runSize);
            if (handle < 0) {
                return false;
            }
        }

        ByteBuffer nioBuffer = cachedNioBuffers != null? cachedNioBuffers.pollLast() : null;
        initBuf(buf, nioBuffer, handle, reqCapacity, cache);// 保存到buf
        return true;
    }

    private long allocateRun(int runSize) {
        int pages = runSize >> pageShifts;// 需要的page个数（page段序号）
        int pageIdx = arena.pages2pageIdx(pages);// 转成page下标（段）

        // 对队列加锁
        synchronized (runsAvail) {
            //find first queue which has at least one big enough run
            int queueIdx = runFirstBestFit(pageIdx);
            // -1好像是没成-》大概是整个chunk都没空闲的
            if (queueIdx == -1) {
                return -1;
            }
            // 拿到对哪个队列操作

            /**
             * chunk中队列：代表当前空闲page数
             * normal在chunk的分配实现：
             * 1. 根据需求page数，找到大于等于 需求的page数 且有handle（代表当前剩余的结果）的队列
             * 2. 有则出队取出handle
             * 3. 扣出需要的page后，挂到对应剩余page数的队列中
             */

            // 即往对应数量的page队列申请
            //get run with min offset in this queue
            LongPriorityQueue queue = runsAvail[queueIdx];
            long handle = queue.poll();// 消费队列

            assert handle != LongPriorityQueue.NO_VALUE && !isUsed(handle) : "invalid handle: " + handle;

            removeAvailRun(queue, handle);// 从队列、map（runofset到handle）移除

            if (handle != -1) {
                // 对当前handle申请实际需要pages -》前面只是找到肯定容纳pages数的handle
                handle = splitLargeRun(handle, pages);// 处理让别线程可以基于这个chunk申请（如果有剩余）
                // 返回是 当前offset| 需求的page数|已用完的handle
                // offset就是本次需要使用的subpage开始下标
            }

            // 空闲扣除本次申请空间总大小byte（实际都是1个个page为单位）
            freeBytes -= runSize(pageShifts, handle);
            return handle;
        }
    }

    private int calculateRunSize(int sizeIdx) {
        // 1个page预留4个做其他用途？最大元素数
        int maxElements = 1 << pageShifts - SizeClasses.LOG2_QUANTUM;
        int runSize = 0;
        int nElements;

        // 元素数量
        final int elemSize = arena.sizeIdx2size(sizeIdx);

        //find lowest common multiple of pageSize and elemSize
        do {
            runSize += pageSize;// runSize是page（8kb）的n倍
            nElements = runSize / elemSize;// 可容纳元素数？
            // nElements=可容纳元素数？
            // elemSize == 单个元素大小？
        } while (nElements < maxElements && runSize != nElements * elemSize);// nElements超过max 或者 runSize是nElements * elemSize 才停
        // 计算直到到
        // nElements > maxElements: 当前runSize可容纳元素超过8192-4个，每个元素可占用1b以上
        // runSize == nElements * elemSize: 元素占用总数等于 page的倍数-》完整page

        // 如果是可容纳元素超过8192-4个，每个元素可占用1b以上 的情况，还原到不超过maxElements数的runSize大小
        while (nElements > maxElements) {
            runSize -= pageSize;// 一直减去page大小，但还是page（8kb）的n倍
            nElements = runSize / elemSize;//
        }

        assert nElements > 0;
        assert runSize <= chunkSize;
        assert runSize >= elemSize;

        return runSize;// 肯定是page（8kb）的n倍
    }

    private int runFirstBestFit(int pageIdx) {
        // 当前整个chunk都空闲 -》初始就只有最后1个page 的队列有元素
        if (freeBytes == chunkSize) {
            return arena.nPSizes - 1;// 大概就是page段最后一个的下标？优先使用最后1个段
        }
        // 从对应pageIdx的段 开始看那个段有空闲
        for (int i = pageIdx; i < arena.nPSizes; i++) {
            // 如果对应page段队列有元素，返回对应段序号
            LongPriorityQueue queue = runsAvail[i];
            if (queue != null && !queue.isEmpty()) {
                return i;
            }
        }
        return -1;
    }

    private long splitLargeRun(long handle, int needPages) {
        assert needPages > 0;

        // 当前handle有的page数
        int totalPages = runPages(handle);
        assert needPages <= totalPages;

        // 扣除需要的page后的剩余page数
        int remPages = totalPages - needPages;

        // 剩余page >0
        if (remPages > 0) {
            int runOffset = runOffset(handle);

            // keep track of trailing unused pages for later use
            // 可用offset = runOffset(初始是0) 加上本次需要page数
            int availOffset = runOffset + needPages;

            // 转成handle -》offset |剩余page数| IS_USED(未用完)
            long availRun = toRunHandle(availOffset, remPages, 0);

            // 往runsAvail队列加入 (remPages段队列加入availRun、映射availOffset到availRun-》用于消费获取)
            // 别的线程可以继续基于这个chunk拿
            insertAvailRun(availOffset, remPages, availRun);

            // 返回1个handle：原runOffset|需要的page|已用完 -》
            // not avail
            return toRunHandle(runOffset, needPages, 1);
        }

        // 剩余全部page已被分配完，更新handle的IS_USED-》即标记全部用完page
        //mark it as used
        handle |= 1L << IS_USED_SHIFT;
        return handle;
    }

    /**
     * Create / initialize a new PoolSubpage of normCapacity. Any PoolSubpage created / initialized here is added to
     * subpage pool in the PoolArena that owns this PoolChunk
     *
     * @param sizeIdx sizeIdx of normalized size
     *
     * @return index in memoryMap
     */
    private long allocateSubpage(int sizeIdx) {
        // Obtain the head of the PoolSubPage pool that is owned by the PoolArena and synchronize on it.
        // This is need as we may add it back and so alter the linked-list structure.
        // 先找下对应大小的全局PoolSubpage，万一并发被其他线程申前
        PoolSubpage<T> head = arena.findSubpagePoolHead(sizeIdx);
        // 全局锁PoolSubpage
        synchronized (head) {
            //allocate a new run
            int runSize = calculateRunSize(sizeIdx);// 计算当前大小的small subpage需要多大空间（多少个page-》8kb），即PoolSubpage的申请空间
            //runSize must be multiples of pageSize

            // 申请normal page
            long runHandle = allocateRun(runSize);

            // 分配失败
            if (runHandle < 0) {
                return -1;
            }

            // 上面的在poolchunk分配成功才能继续

            // subpage在poolchunk的 subpage开始下标——》可能需要多个page
            int runOffset = runOffset(runHandle);
            assert subpages[runOffset] == null;
            int elemSize = arena.sizeIdx2size(sizeIdx);// 需要的对齐后的大小

            // 真正创建PoolSubpage加入到area的链表
            PoolSubpage<T> subpage = new PoolSubpage<T>(head, this, pageShifts, runOffset,
                               runSize(pageShifts, runHandle), elemSize);

            // 在poolchunk中记录subpage
            subpages[runOffset] = subpage;
            // 上面是新增初始化subpages
            // 这里是使用subpages：执行在subpages分配处理 -》申请大小就是elemSize，即对齐后的需求大小
            return subpage.allocate();
        }
    }

    /**
     * Free a subpage or a run of pages When a subpage is freed from PoolSubpage, it might be added back to subpage pool
     * of the owning PoolArena. If the subpage pool in PoolArena has at least one other PoolSubpage of given elemSize,
     * we can completely free the owning Page so it is available for subsequent allocations
     *
     * @param handle handle to free
     */
    void free(long handle, int normCapacity, ByteBuffer nioBuffer) {
        if (isSubpage(handle)) {
            int sizeIdx = arena.size2SizeIdx(normCapacity);
            PoolSubpage<T> head = arena.findSubpagePoolHead(sizeIdx);

            int sIdx = runOffset(handle);
            PoolSubpage<T> subpage = subpages[sIdx];
            assert subpage != null && subpage.doNotDestroy;

            // Obtain the head of the PoolSubPage pool that is owned by the PoolArena and synchronize on it.
            // This is need as we may add it back and so alter the linked-list structure.
            synchronized (head) {
                if (subpage.free(head, bitmapIdx(handle))) {
                    //the subpage is still used, do not free it
                    return;
                }
                assert !subpage.doNotDestroy;
                // Null out slot in the array as it was freed and we should not use it anymore.
                subpages[sIdx] = null;
            }
        }

        //start free run
        int pages = runPages(handle);

        synchronized (runsAvail) {
            // collapse continuous runs, successfully collapsed runs
            // will be removed from runsAvail and runsAvailMap
            long finalRun = collapseRuns(handle);

            //set run as not used
            finalRun &= ~(1L << IS_USED_SHIFT);
            //if it is a subpage, set it to run
            finalRun &= ~(1L << IS_SUBPAGE_SHIFT);

            insertAvailRun(runOffset(finalRun), runPages(finalRun), finalRun);
            freeBytes += pages << pageShifts;
        }

        if (nioBuffer != null && cachedNioBuffers != null &&
            cachedNioBuffers.size() < PooledByteBufAllocator.DEFAULT_MAX_CACHED_BYTEBUFFERS_PER_CHUNK) {
            cachedNioBuffers.offer(nioBuffer);
        }
    }

    private long collapseRuns(long handle) {
        return collapseNext(collapsePast(handle));
    }

    private long collapsePast(long handle) {
        for (;;) {
            int runOffset = runOffset(handle);
            int runPages = runPages(handle);

            long pastRun = getAvailRunByOffset(runOffset - 1);
            if (pastRun == -1) {
                return handle;
            }

            int pastOffset = runOffset(pastRun);
            int pastPages = runPages(pastRun);

            //is continuous
            if (pastRun != handle && pastOffset + pastPages == runOffset) {
                //remove past run
                removeAvailRun(pastRun);
                handle = toRunHandle(pastOffset, pastPages + runPages, 0);
            } else {
                return handle;
            }
        }
    }

    private long collapseNext(long handle) {
        for (;;) {
            int runOffset = runOffset(handle);
            int runPages = runPages(handle);

            long nextRun = getAvailRunByOffset(runOffset + runPages);
            if (nextRun == -1) {
                return handle;
            }

            int nextOffset = runOffset(nextRun);
            int nextPages = runPages(nextRun);

            //is continuous
            if (nextRun != handle && runOffset + runPages == nextOffset) {
                //remove next run
                removeAvailRun(nextRun);
                handle = toRunHandle(runOffset, runPages + nextPages, 0);
            } else {
                return handle;
            }
        }
    }

    private static long toRunHandle(int runOffset, int runPages, int inUsed) {
        return (long) runOffset << RUN_OFFSET_SHIFT
               | (long) runPages << SIZE_SHIFT
               | (long) inUsed << IS_USED_SHIFT;
    }

    void initBuf(PooledByteBuf<T> buf, ByteBuffer nioBuffer, long handle, int reqCapacity,
                 PoolThreadCache threadCache) {
        if (isRun(handle)) {
            buf.init(this, nioBuffer, handle, runOffset(handle) << pageShifts,
                     reqCapacity, runSize(pageShifts, handle), arena.parent.threadCache());
        } else {
            initBufWithSubpage(buf, nioBuffer, handle, reqCapacity, threadCache);
        }
    }

    void initBufWithSubpage(PooledByteBuf<T> buf, ByteBuffer nioBuffer, long handle, int reqCapacity,
                            PoolThreadCache threadCache) {
        int runOffset = runOffset(handle);
        int bitmapIdx = bitmapIdx(handle);

        PoolSubpage<T> s = subpages[runOffset];
        assert s.doNotDestroy;
        assert reqCapacity <= s.elemSize;

        // 得到具体下标（ chunk subpage数组的offset + bitmapIdx段内部对应开始）
        int offset = (runOffset << pageShifts) + bitmapIdx * s.elemSize;

        // 把这些 记录更新到buf中
        buf.init(this, nioBuffer, handle, offset, reqCapacity, s.elemSize, threadCache);
    }

    @Override
    public int chunkSize() {
        return chunkSize;
    }

    @Override
    public int freeBytes() {
        synchronized (arena) {
            return freeBytes;
        }
    }

    @Override
    public String toString() {
        final int freeBytes;
        synchronized (arena) {
            freeBytes = this.freeBytes;
        }

        return new StringBuilder()
                .append("Chunk(")
                .append(Integer.toHexString(System.identityHashCode(this)))
                .append(": ")
                .append(usage(freeBytes))
                .append("%, ")
                .append(chunkSize - freeBytes)
                .append('/')
                .append(chunkSize)
                .append(')')
                .toString();
    }

    void destroy() {
        arena.destroyChunk(this);
    }

    static int runOffset(long handle) {
        return (int) (handle >> RUN_OFFSET_SHIFT);
    }

    static int runSize(int pageShifts, long handle) {
        // runPages：27位page数
        //  << pageShifts：总大小
        return runPages(handle) << pageShifts;
    }

    static int runPages(long handle) {
        // 右移34位，的15位（page）
        return (int) (handle >> SIZE_SHIFT & 0x7fff);
    }

    static boolean isUsed(long handle) {
        return (handle >> IS_USED_SHIFT & 1) == 1L;
    }

    static boolean isRun(long handle) {
        return !isSubpage(handle);
    }

    static boolean isSubpage(long handle) {
        return (handle >> IS_SUBPAGE_SHIFT & 1) == 1L;
    }

    static int bitmapIdx(long handle) {
        return (int) handle;
    }
}
