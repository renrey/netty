/*
 * Copyright 2020 The Netty Project
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

import static io.netty.buffer.PoolThreadCache.*;

/**
 * SizeClasses requires {@code pageShifts} to be defined prior to inclusion,
 * and it in turn defines:
 * <p>
 *   LOG2_SIZE_CLASS_GROUP: Log of size class count for each size doubling.
 *   LOG2_MAX_LOOKUP_SIZE: Log of max size class in the lookup table.
 *   sizeClasses: Complete table of [index, log2Group, log2Delta, nDelta, isMultiPageSize,
 *                 isSubPage, log2DeltaLookup] tuples.
 *     index: Size class index.
 *     log2Group: Log of group base size (no deltas added).
 *     log2Delta: Log of delta to previous size class.
 *     nDelta: Delta multiplier.
 *     isMultiPageSize: 'yes' if a multiple of the page size, 'no' otherwise.
 *     isSubPage: 'yes' if a subpage size class, 'no' otherwise.
 *     log2DeltaLookup: Same as log2Delta if a lookup table size class, 'no'
 *                      otherwise.
 * <p>
 *   nSubpages: Number of subpages size classes.
 *   nSizes: Number of size classes.
 *   nPSizes: Number of size classes that are multiples of pageSize.
 *
 *   smallMaxSizeIdx: Maximum small size class index.
 *
 *   lookupMaxclass: Maximum size class included in lookup table.
 *   log2NormalMinClass: Log of minimum normal size class.
 * <p>
 *   The first size class and spacing are 1 << LOG2_QUANTUM.
 *   Each group has 1 << LOG2_SIZE_CLASS_GROUP of size classes.
 *
 *   size = 1 << log2Group + nDelta * (1 << log2Delta)
 *
 *   The first size class has an unusual encoding, because the size has to be
 *   split between group and delta*nDelta.
 *
 *   If pageShift = 13, sizeClasses looks like this:
 * 小于32kb的，前n个都是isSubPage=yes
 * 小于2kb的有log2DeltaLookup，且log2DeltaLookup=log2Delta
 * 8kb后的index，isMultiPageSize=yes
 *   (index, log2Group, log2Delta, nDelta, isMultiPageSize, isSubPage, log2DeltaLookup)
 * <p>
 *   ( 0,     4,        4,         0,       no,             yes,        4)
 *   ( 1,     4,        4,         1,       no,             yes,        4)
 *   ( 2,     4,        4,         2,       no,             yes,        4)
 *   ( 3,     4,        4,         3,       no,             yes,        4)
 * <p>
 *   ( 4,     6,        4,         1,       no,             yes,        4)
 *   ( 5,     6,        4,         2,       no,             yes,        4)
 *   ( 6,     6,        4,         3,       no,             yes,        4)
 *   ( 7,     6,        4,         4,       no,             yes,        4)
 * <p>
 *   ( 8,     7,        5,         1,       no,             yes,        5)
 *   ( 9,     7,        5,         2,       no,             yes,        5)
 *   ( 10,    7,        5,         3,       no,             yes,        5)
 *   ( 11,    7,        5,         4,       no,             yes,        5)
 *   ...
 *   ...
 *   ( 72,    23,       21,        1,       yes,            no,        no)
 *   ( 73,    23,       21,        2,       yes,            no,        no)
 *   ( 74,    23,       21,        3,       yes,            no,        no)
 *   ( 75,    23,       21,        4,       yes,            no,        no)
 * <p>
 *   ( 76,    24,       22,        1,       yes,            no,        no)
 */
abstract class SizeClasses implements SizeClassesMetric {

    static final int LOG2_QUANTUM = 4;

    private static final int LOG2_SIZE_CLASS_GROUP = 2;
    private static final int LOG2_MAX_LOOKUP_SIZE = 12;

    private static final int INDEX_IDX = 0;
    private static final int LOG2GROUP_IDX = 1;
    private static final int LOG2DELTA_IDX = 2;
    private static final int NDELTA_IDX = 3;
    private static final int PAGESIZE_IDX = 4;
    private static final int SUBPAGE_IDX = 5;
    private static final int LOG2_DELTA_LOOKUP_IDX = 6;

    private static final byte no = 0, yes = 1;

    protected SizeClasses(int pageSize, int pageShifts, int chunkSize, int directMemoryCacheAlignment) {
        this.pageSize = pageSize;
        this.pageShifts = pageShifts;
        this.chunkSize = chunkSize;
        this.directMemoryCacheAlignment = directMemoryCacheAlignment;

        // 计算有（16m（25位）+1-4）==22
        int group = log2(chunkSize) + 1 - LOG2_QUANTUM;

        // 生成二维数组 长度为group的4倍=88，二维为7个属性
        //generate size classes
        // 二维属性：[index, log2Group, log2Delta, nDelta, isMultiPageSize, isSubPage, log2DeltaLookup]
        sizeClasses = new short[group << LOG2_SIZE_CLASS_GROUP][7];
        nSizes = sizeClasses();// 有多少个真正的（有数据的）一维，77个
        // 里面包含small（index是smallMaxSizeIdx前的，属性=subpage）的跟normal的

        // 生成寻址表
        //generate lookup table
        sizeIdx2sizeTab = new int[nSizes];// 通过目标大小计算得到sizeIdx，找对应位置在sizeTab的值
        pageIdx2sizeTab = new int[nPSizes];
        idx2SizeTab(sizeIdx2sizeTab, pageIdx2sizeTab);

        size2idxTab = new int[lookupMaxSize >> LOG2_QUANTUM];
        size2idxTab(size2idxTab);
    }

    protected final int pageSize;
    protected final int pageShifts;// page前缀的二进制位数（移除pageSize的后n位）
    protected final int chunkSize;// 16MB
    protected final int directMemoryCacheAlignment;

    final int nSizes;//INDEX数
    int nSubpages;// 用subPage数
    int nPSizes;// 用到完整page（8kb）数量

    // nSubpages+nPSizes = nSizes

    int smallMaxSizeIdx;// 小于32kb（page的4倍）的index最大下标

    private int lookupMaxSize;// 可用lookup的最大size值（不超过2kb）

    private final short[][] sizeClasses;

    private final int[] pageIdx2sizeTab;

    // lookup table for sizeIdx <= smallMaxSizeIdx
    private final int[] sizeIdx2sizeTab;

    // lookup table used for size <= lookupMaxclass
    // spacing is 1 << LOG2_QUANTUM, so the size of array is lookupMaxclass >> LOG2_QUANTUM
    private final int[] size2idxTab;

    private int sizeClasses() {
        int normalMaxSize = -1;

        int index = 0;
        int size = 0;


        int log2Group = LOG2_QUANTUM;// 组开始值
        int log2Delta = LOG2_QUANTUM;// 当前组的填充单位大小
        int ndeltaLimit = 1 << LOG2_SIZE_CLASS_GROUP;// 每组占4个


        //First small group, nDelta start at 0. 首先是是small group，nDelta从0开始
        //first size class is 1 << LOG2_QUANTUM
        // 第1组，大小未超过64b的 -》small
        int nDelta = 0;// 第1组的填充个数从0开始，即0到3个单位
        while (nDelta < ndeltaLimit) {// 4次
            size = sizeClass(index++, log2Group, log2Delta, nDelta++);// size index+1，nDelta+1
            // nDelta位复用的，4次分别是0到3
            // index跟nDelta一样，都是0到3
            // 前4个index（第1组） 的 log2Group、log2Delta 都是4，代表的size是16、32、48、64（1个组的正常大小）
        }
        // 第1组的大小就是 64分开4份


        // 第二组，大小超过64的
        log2Group += LOG2_SIZE_CLASS_GROUP;// =6 （+了2次），此时log2Delta比1log2Group小2
        // size 就申请空间大小b

        // 剩下的nDelta从1开始
        //All remaining groups, nDelta start at 1.
        while (size < chunkSize) {// 当前size未超过16Mb
            nDelta = 1;// 每次从1开始 （填充单位个数）

            // 第2组（index从4到7）：group=6（64b），log2Delta=4（16b），nDelta从1到4，size = 80到 128b
            // 第3组（index 8到11）：group=7（128b），log2Delta=4（32b），size= 160到256
            // 也跟上面也是循环4次，直到 当前size超过16Mb
            while (nDelta <= ndeltaLimit && size < chunkSize) {
                size = sizeClass(index++, log2Group, log2Delta, nDelta++);// size index+1（正常下标），nDelta+1（个数+1）
                normalMaxSize = size;
            }
            // 最后index是79，因为group=24（8M），log2Delta=22（2M）得到的size=16

            log2Group++;// 下一组
            log2Delta++;// 下一组的填充单位大小+1，实际计算最大就是page大小
        }
        // 所以第2组开始的：从64(1个组的单位)开始，每组（4个）大小从64*（n-1）*(1.25)到64*（n-1）*2，即往前1组最大大小（64的倍数）*按4分1加上去

        //chunkSize must be normalMaxSize
        assert chunkSize == normalMaxSize;  // 其实最后normalMaxSize应该等于chunkSize（16M）

        //return number of size index
        return index;// 最后的size index下标 -》数量
    }

    //calculate size class
    private int sizeClass(int index, int log2Group, int log2Delta, int nDelta) {
        short isMultiPageSize;
        // 代表log2Delta表示填充单位，nDelta是用到多少个这个单位（1到4个）
        if (log2Delta >= pageShifts) {// >=13，超过1个pagesize
            isMultiPageSize = yes;
        } else {
            // 小于 pagesize
            int pageSize = 1 << pageShifts;// 转回page大小（8kb）

            //
            int size = (1 << log2Group) + (1 << log2Delta) * nDelta;// 等于往group值+nDelta*delta （注意都是移动对应位后），
            // index=0时，size=16+16*0=16，INDEX 从1到3，size则是32、48、64
            // 每个4index1组，4到7，size=64（log2Group）+4（log2Delta）*4 ，size从80到92

            // size是pagesize（8kb）的倍数，例如16k、32k、64k、72k、80k
            // 就是size能整齐分配page(无多出空间) 就是yes
            // 8kb后的index，isMultiPageSize=yes
            isMultiPageSize = size == size / pageSize * pageSize? yes : no;
        }

        int log2Ndelta = nDelta == 0? 0 : log2(nDelta);// 填充单位数量的 位数

        byte remove = 1 << log2Ndelta < nDelta? yes : no;

        // 反正得到size的位数
        int log2Size = log2Delta + log2Ndelta == log2Group? log2Group + 1 : log2Group;
        if (log2Size == log2Group) {
            remove = yes;
        }

        // 子page 判断log2Size（size位数）小于 16（没到达page+组大小）-》小于32kb（page的4倍），是subPage
        short isSubpage = log2Size < pageShifts + LOG2_SIZE_CLASS_GROUP? yes : no;

        // size位数小于12（2kb），log2Delta（填充单位），大于（大于2kb不用lookup）就是0
        int log2DeltaLookup = log2Size < LOG2_MAX_LOOKUP_SIZE ||
                              log2Size == LOG2_MAX_LOOKUP_SIZE && remove == no
                ? log2Delta : no;

        // 生成元素数组
        // sz就是7个元素
        short[] sz = {
                (short) index, (short) log2Group, (short) log2Delta,
                (short) nDelta, isMultiPageSize, isSubpage, (short) log2DeltaLookup
        };

        // index位置存sz
        sizeClasses[index] = sz;

        // size 等于2的（log2Group-1）次方 + nDelta * 2的（log2Delta）次
        int size = (1 << log2Group) + (nDelta << log2Delta);

        // 根据sz对应位置的值，对统计修改
        // 是pagesize
        if (sz[PAGESIZE_IDX] == yes) {//第5个
            nPSizes++;// pagesize+1，用到完整page（isMultiPageSize=yes）数量+1
        }
        // 是subpage
        if (sz[SUBPAGE_IDX] == yes) {// 第6个
            nSubpages++;//subpage数+1
            smallMaxSizeIdx = index;// 更新small的最大index, 即当前SizeIdx之前的都是subpage
        }
        // 大概
        if (sz[LOG2_DELTA_LOOKUP_IDX] != no) {// 第7个
            lookupMaxSize = size;// 全局lookup的最大大小 = 当前size，不超过2kb
        }

        return size;
    }

    private void idx2SizeTab(int[] sizeIdx2sizeTab, int[] pageIdx2sizeTab) {
        int pageIdx = 0;

        for (int i = 0; i < nSizes; i++) {
            short[] sizeClass = sizeClasses[i];
            int log2Group = sizeClass[LOG2GROUP_IDX];
            int log2Delta = sizeClass[LOG2DELTA_IDX];
            int nDelta = sizeClass[NDELTA_IDX];

            // small的第1个：log2Group=4，log2Delta=4，nDelta=0，即16b,第2个：32b,第3个：48 ,第4个：64
            int size = (1 << log2Group) + (nDelta << log2Delta);
            sizeIdx2sizeTab[i] = size;// 赋值

            if (sizeClass[PAGESIZE_IDX] == yes) {
                pageIdx2sizeTab[pageIdx++] = size;
            }
        }
    }

    private void size2idxTab(int[] size2idxTab) {
        int idx = 0;
        int size = 0;

        for (int i = 0; size <= lookupMaxSize; i++) {
            int log2Delta = sizeClasses[i][LOG2DELTA_IDX];
            int times = 1 << log2Delta - LOG2_QUANTUM;

            while (size <= lookupMaxSize && times-- > 0) {
                size2idxTab[idx++] = i;
                size = idx + 1 << LOG2_QUANTUM;
            }
        }
    }

    @Override
    public int sizeIdx2size(int sizeIdx) {
        // 通过sizeIdx找sizeTab 中存放的值
        return sizeIdx2sizeTab[sizeIdx];
    }

    @Override
    public int sizeIdx2sizeCompute(int sizeIdx) {
        int group = sizeIdx >> LOG2_SIZE_CLASS_GROUP;
        int mod = sizeIdx & (1 << LOG2_SIZE_CLASS_GROUP) - 1;

        int groupSize = group == 0? 0 :
                1 << LOG2_QUANTUM + LOG2_SIZE_CLASS_GROUP - 1 << group;

        int shift = group == 0? 1 : group;
        int lgDelta = shift + LOG2_QUANTUM - 1;
        int modSize = mod + 1 << lgDelta;

        return groupSize + modSize;
    }

    @Override
    public long pageIdx2size(int pageIdx) {
        return pageIdx2sizeTab[pageIdx];
    }

    @Override
    public long pageIdx2sizeCompute(int pageIdx) {
        int group = pageIdx >> LOG2_SIZE_CLASS_GROUP;
        int mod = pageIdx & (1 << LOG2_SIZE_CLASS_GROUP) - 1;

        long groupSize = group == 0? 0 :
                1L << pageShifts + LOG2_SIZE_CLASS_GROUP - 1 << group;

        int shift = group == 0? 1 : group;
        int log2Delta = shift + pageShifts - 1;
        int modSize = mod + 1 << log2Delta;

        return groupSize + modSize;
    }

    @Override
    public int size2SizeIdx(int size) {
        if (size == 0) {
            return 0;
        }
        if (size > chunkSize) {// 需求大小超过1个chunk大小 （16M）
            return nSizes;// 就是超过chunk
        }

        if (directMemoryCacheAlignment > 0) {
            size = alignSize(size);
        }

        if (size <= lookupMaxSize) {
            //size-1 / MIN_TINY
            return size2idxTab[size - 1 >> LOG2_QUANTUM];
        }

        // 这里log2的作用 -》计算size是多少（包含最高位1）位
        // (size << 1) - 1 作用是确保最后1位是0？
        int x = log2((size << 1) - 1);

        // 其实就是判断size是否超过64b（小于组只能是0）
        // 右边小于7位，shift=0（不算是page，只能算是subPage）， 超过则是(64b) 去掉6个 即保留的组前缀
        int shift = x < LOG2_SIZE_CLASS_GROUP + LOG2_QUANTUM + 1
                ? 0 : x - (LOG2_SIZE_CLASS_GROUP + LOG2_QUANTUM);

        // 序号中前面的为group，剩下2位作为mod？（1个group前缀有4个数）
        // 组=shift*4（用于保证下面的2进值的低2位=0，所以shift本身就是group前缀值）
        int group = shift << LOG2_SIZE_CLASS_GROUP;// 低2位肯定0


        // delta （跟shift一起？）x小于7（64b）: 固定4 （低6位中前2位作为mod，低6到5）   超过：x-3 (低4到5位)
        // 通过下面mod计计算的右移可知，用于去掉多少位
        int log2Delta = x < LOG2_SIZE_CLASS_GROUP + LOG2_QUANTUM + 1
                ? LOG2_QUANTUM : x - LOG2_SIZE_CLASS_GROUP - 1;

        // 前面全1，低log2Delta全是0-》用于清除delta位？
        int deltaInverseMask = -1 << log2Delta;

        // 就是size在当前组序号
        // &deltaInverseMask再右移 -》跟直接右移log2Delta位 一样。。
        // 再&就是保留LOG2_SIZE_CLASS_GROUP（2位）-》，mod最后4个可能（-1-2）
        int mod = (size - 1 & deltaInverseMask) >> log2Delta &
                  (1 << LOG2_SIZE_CLASS_GROUP) - 1;

        // 组合：前30位：group（低2位肯定0，64的倍数，多少个64页） + 低2位：mod（看log2Delta）
        return group + mod;
    }

    @Override
    public int pages2pageIdx(int pages) {
        return pages2pageIdxCompute(pages, false);
    }

    @Override
    public int pages2pageIdxFloor(int pages) {
        return pages2pageIdxCompute(pages, true);
    }

    private int pages2pageIdxCompute(int pages, boolean floor) {
        int pageSize = pages << pageShifts;// page的真实大小
        // 超过chunk，返回page数，即最后的下标
        if (pageSize > chunkSize) {
            return nPSizes;
        }

        // 得到pageSize占用多少位
        int x = log2((pageSize << 1) - 1);

        // LOG2_SIZE_CLASS_GROUP + pageShifts -》等于page的占用位数+2
        // 即用段的位数= 从page的占用位数+2位 开始向前
        int shift = x < LOG2_SIZE_CLASS_GROUP + pageShifts
                ? 0 : x - (LOG2_SIZE_CLASS_GROUP + pageShifts);

        int group = shift << LOG2_SIZE_CLASS_GROUP;// 转化下标使用，低2位用mod

        int log2Delta = x < LOG2_SIZE_CLASS_GROUP + pageShifts + 1?
                pageShifts : x - LOG2_SIZE_CLASS_GROUP - 1;

        int deltaInverseMask = -1 << log2Delta;
        int mod = (pageSize - 1 & deltaInverseMask) >> log2Delta &
                  (1 << LOG2_SIZE_CLASS_GROUP) - 1;

        // 整合成具体下标数字
        int pageIdx = group + mod;

        if (floor && pageIdx2sizeTab[pageIdx] > pages << pageShifts) {
            pageIdx--;
        }

        return pageIdx;
    }

    // Round size up to the nearest multiple of alignment.
    private int alignSize(int size) {
        int delta = size & directMemoryCacheAlignment - 1;
        return delta == 0? size : size + directMemoryCacheAlignment - delta;
    }

    @Override
    public int normalizeSize(int size) {
        if (size == 0) {
            return sizeIdx2sizeTab[0];
        }
        if (directMemoryCacheAlignment > 0) {
            size = alignSize(size);
        }

        if (size <= lookupMaxSize) {
            int ret = sizeIdx2sizeTab[size2idxTab[size - 1 >> LOG2_QUANTUM]];
            assert ret == normalizeSizeCompute(size);
            return ret;
        }
        return normalizeSizeCompute(size);
    }

    private static int normalizeSizeCompute(int size) {
        int x = log2((size << 1) - 1);
        int log2Delta = x < LOG2_SIZE_CLASS_GROUP + LOG2_QUANTUM + 1
                ? LOG2_QUANTUM : x - LOG2_SIZE_CLASS_GROUP - 1;
        int delta = 1 << log2Delta;
        int delta_mask = delta - 1;
        return size + delta_mask & ~delta_mask;
    }
}
