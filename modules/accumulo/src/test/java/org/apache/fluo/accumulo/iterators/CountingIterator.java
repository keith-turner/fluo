package org.apache.fluo.accumulo.iterators;

import java.io.IOException;
import java.util.Collection;
import java.util.SortedMap;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedMapIterator;

public class CountingIterator extends SortedMapIterator {

  public static class Counter {
    public int nextCalls;
    public int seeks;

    public void reset() {
      nextCalls = 0;
      seeks = 0;
    }
  }

  private Counter counter;

  CountingIterator(Counter counter, SortedMap<Key, Value> data) {
    super(data);
    this.counter = counter;
  }

  @Override
  public void next() throws IOException {
    super.next();
    if (counter != null) {
      counter.nextCalls++;
    }
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
      throws IOException {
    super.seek(range, columnFamilies, inclusive);
    if (counter != null) {
      counter.seeks++;
    }
  }
}
