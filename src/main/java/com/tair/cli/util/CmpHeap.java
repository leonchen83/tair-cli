/*
 * Copyright 2018-2019 Baoyi Chen
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tair.cli.util;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Consumer;

/**
 * @author Baoyi Chen
 */
public class CmpHeap<T extends Comparable<T>> {
    
    private final int n;
    private final List<T> ary;
    private Consumer<T> consumer;
    
    public void setConsumer(Consumer<T> consumer) {
        this.consumer = consumer;
    }
    
    public CmpHeap(int n) {
        this.n = n;
        this.ary = new ArrayList<>();
    }
    
    private void heapify(List<T> ary, int idx) {
        int l = idx == 0 ? 1 : (idx << 1);
        int r = idx == 0 ? 2 : (idx << 1) + 1;
        int min = idx;
        if (l < ary.size()) {
            if (ary.get(l).compareTo(ary.get(idx)) < 0) {
                min = l;
            }
        }
        if (r < ary.size()) {
            if (ary.get(r).compareTo(ary.get(min)) < 0) {
                min = r;
            }
        }
        if (min != idx) {
            T t1 = ary.get(idx);
            T t2 = ary.get(min);
            ary.set(idx, t2);
            ary.set(min, t1);
            heapify(ary, min);
        }
    }
    
    private void build(List<T> a) {
        for (int i = (a.size() - 1) / 2; i >= 0; i--) {
            heapify(a, i);
        }
    }
    
    public void add(T t) {
        if (n <= 0) {
            if (consumer != null) consumer.accept(t);
            return;
        }
        if (ary.size() < n) {
            ary.add(t);
            return;
        }
        if (ary.size() == n) {
            build(ary);
        }
        if (ary.get(0).compareTo(t) < 0) {
            ary.set(0, t);
            heapify(ary, 0);
        }
    }
    
    public List<T> get(boolean sort) {
        if (sort) ary.sort(Comparator.reverseOrder());
        return ary;
    }
}
