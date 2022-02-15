/*
 * Copyright 2016-2017 Leon Chen
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

package com.tair.cli.monitor.impl;

import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

import com.tair.cli.monitor.entity.Counter;
import com.moilioncircle.redis.replicator.util.Tuples;
import com.moilioncircle.redis.replicator.util.type.Tuple2;


/**
 * @author Jingqi Xu
 */
public final class XCounter implements Counter {
    private Slot next = new Slot();
    private final AtomicReference<Slot> slot = new AtomicReference<>(new Slot());

    @Override
    public Tuple2<Long, Long> getCounter() {
        return this.slot.get().getCounter(false);
    }

    @Override
    public synchronized Counter reset() {
        Slot prev = this.slot.getAndSet(this.next);
        try {
            Tuple2<Long, Long> v = prev.getCounter(true);
            return new ImmutableCounter(v);
        } finally {
            prev.reset();
            this.next = prev;
        }
    }

    void add(long count, long time) {
        Slot v = slot.get();
        v.add(count, time);
    }

    private static final class Slot {
        private final LongAdder v1 = new LongAdder(), v2 = new LongAdder();

        private void reset() {
            v1.reset();
            v2.reset();
        }

        private Tuple2<Long, Long> getCounter(boolean reset) {
            long n = reset ? v1.sumThenReset() : v1.sum();
            long t = reset ? v2.sumThenReset() : v2.sum();
            Tuple2<Long, Long> r = Tuples.of(n, t);
            return r;
        }

        private void add(long n, long t) {
            if (n > 0L) this.v1.add(n);
            if (t > 0L) this.v2.add(t);
        }
    }

    private static class ImmutableCounter implements Counter {
        private final Tuple2<Long, Long> value;

        private ImmutableCounter(Tuple2<Long, Long> value) {
            this.value = value;
        }

        @Override
        public Counter reset() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Tuple2<Long, Long> getCounter() {
            return Tuples.of(value.getV1(), value.getV2());
        }
    }
}
