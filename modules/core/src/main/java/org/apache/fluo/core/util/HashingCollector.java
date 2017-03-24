package org.apache.fluo.core.util;

import java.util.Collections;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

public class HashingCollector<T> implements Collector<T,Hasher,HashCode>{

  private HashFunction hashFunc;
  private BiConsumer<Hasher,T> accumulator;

  public HashingCollector(HashFunction hashFunc, BiConsumer<Hasher,T> accumulator) {
    this.hashFunc = hashFunc;
    this.accumulator = accumulator;
  }
  
  @Override
  public Supplier<Hasher> supplier() {
    return hashFunc::newHasher;
  }

  @Override
  public BiConsumer<Hasher,T> accumulator() {
    return accumulator;
  }

  @Override
  public BinaryOperator<Hasher> combiner() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Function<Hasher,HashCode> finisher() {
    return Hasher::hash;
  }

  @Override
  public Set<java.util.stream.Collector.Characteristics> characteristics() {
    return Collections.emptySet();
  }
  
  public static <T2> HashingCollector<T2> murmur3_32(BiConsumer<Hasher,T2> accumulator){
    return new HashingCollector<>(Hashing.murmur3_32(), accumulator);
  }
}
