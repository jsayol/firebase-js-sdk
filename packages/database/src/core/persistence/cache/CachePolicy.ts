/**
 * Copyright 2018 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * An interface defining a cache policy
 */
export interface CachePolicy {
  percentQueriesPruneAtOnce(): number;
  maxPrunableQueriesToKeep(): number;
  shouldPrune(cacheSize: number, numTrackedQueries: number): boolean;
  shouldCheckSize(numServerUpdates: number): boolean;
}

/**
 * A "Least Recently Used" cache policy
 */
export class LRUCachePolicy implements CachePolicy {
  private static readonly PERCENT_QUERIES_PRUNE_AT_ONCE_ = 0.2;
  private static readonly SERVER_UPDATES_BETWEEN_SIZE_CHECKS_ = 1000;
  private static readonly MAX_PRUNABLE_QUERIES_TO_KEEP_ = 1000;

  /**
   * Default maximum cache size to use, in bytes.
   */
  private static readonly DEFAULT_MAX_CACHE_SIZE_ = 10 * 1024 * 1024;

  constructor(private maxSize_ = LRUCachePolicy.DEFAULT_MAX_CACHE_SIZE_) {}

  percentQueriesPruneAtOnce() {
    return LRUCachePolicy.PERCENT_QUERIES_PRUNE_AT_ONCE_;
  }

  maxPrunableQueriesToKeep() {
    return LRUCachePolicy.MAX_PRUNABLE_QUERIES_TO_KEEP_;
  }

  shouldPrune(cacheSize: number, numTrackedQueries: number): boolean {
    return (
      cacheSize > this.maxSize_ ||
      numTrackedQueries > LRUCachePolicy.MAX_PRUNABLE_QUERIES_TO_KEEP_
    );
  }

  shouldCheckSize(numServerUpdates: number): boolean {
    return (
      numServerUpdates > LRUCachePolicy.SERVER_UPDATES_BETWEEN_SIZE_CHECKS_
    );
  }
}

/**
 * A cache policy that never prunes, used for tests
 */
export class TestCachePolicy implements CachePolicy {
  percentQueriesPruneAtOnce() {
    return 0;
  }

  maxPrunableQueriesToKeep() {
    return Number.POSITIVE_INFINITY;
  }

  shouldPrune(cacheSize: number, numTrackedQueries: number): boolean {
    return false;
  }

  shouldCheckSize(numServerUpdates: number): boolean {
    return false;
  }
}
