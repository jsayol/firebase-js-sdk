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

import { Query } from '../../../api/Query';
import { Path } from '../../util/Path';
import { log, warn } from '../../util/util';
import { ImmutableTree } from '../../util/ImmutableTree';
import { assert, forEach } from '@firebase/util';
import { CachePolicy } from '../cache/CachePolicy';
import { PruneForest } from '../cache/PruneForest';
import { TrackedQuery } from './TrackedQuery';
import { TrackedQueryStore } from './TrackedQueryStore';

interface TrackedQueryMap {
  [queryId: string]: TrackedQuery;
}

const normalizeQuery = (query: Query) => {
  return query.getQueryParams().loadsAllData()
    ? Query.defaultAtPath(query.path, query.repo)
    : query;
};

const numQueriesToPrune = (
  cachePolicy: CachePolicy,
  numPrunable: number
): number => {
  const numPercent = Math.ceil(
    numPrunable * cachePolicy.percentQueriesPruneAtOnce()
  );
  const maxToKeep = cachePolicy.maxPrunableQueriesToKeep();
  const numMax = numPrunable > maxToKeep ? numPrunable - maxToKeep : 0;

  return Math.max(numMax, numPercent);
};

const boundLog = log.bind(null, 'TrackedQueryManager:') as (
  ...args: any[]
) => void;

export class TrackedQueryManager {
  initialized: Promise<void>;

  private nextId = 0;
  private trackedQueryTree_: ImmutableTree<
    TrackedQueryMap
  > = ImmutableTree.Empty;

  constructor(private trackedQueryStore_: TrackedQueryStore) {
    this.initialized = this.trackedQueryStore_
      .load()
      .then((trackedQueries: TrackedQuery[]) => {
        const lastUse = Date.now();

        trackedQueries.forEach((trackedQuery: TrackedQuery) => {
          this.nextId = Math.max(trackedQuery.id + 1, this.nextId);
          if (trackedQuery.active) {
            trackedQuery.active = false;
            trackedQuery.lastUse = lastUse;
            boundLog(
              `inactivating query ${trackedQuery.id} from previous app start`
            );
            this.trackedQueryStore_.save(trackedQuery);
          }
          this.cacheTrackedQuery_(trackedQuery);
        });
      });
  }

  find(query: Query): TrackedQuery {
    query = normalizeQuery(query);
    const trackedQueryMap = this.trackedQueryTree_.get(query.path) || {};
    return trackedQueryMap[query.queryIdentifier()];
  }

  remove(query: Query) {
    query = normalizeQuery(query);
    const trackedQuery = this.find(query);
    assert(trackedQuery, 'Tracked query must exist to be removed');

    this.trackedQueryStore_.removeKeys([trackedQuery.id]);
    const trackedQueryMap = this.trackedQueryTree_.get(query.path);
    delete trackedQueryMap[query.queryIdentifier()];
  }

  setActive(query: Query) {
    this.setActiveState_(query, true);
  }

  setInactive(query: Query) {
    this.setActiveState_(query, false);
  }

  private setActiveState_(query: Query, active: boolean) {
    this.initialized.then(() => {
      query = normalizeQuery(query);
      let trackedQuery = this.find(query);
      const lastUse = Date.now();

      if (trackedQuery) {
        trackedQuery.active = active;
        trackedQuery.lastUse = lastUse;
      } else {
        assert(
          active,
          "If we're setting the query to inactive, we should already be tracking it"
        );
        trackedQuery = new TrackedQuery(this.nextId++, query, lastUse, active);
        this.cacheTrackedQuery_(trackedQuery);
      }

      boundLog(`setActiveState id=${trackedQuery.id} active=${active}`);
      this.trackedQueryStore_.save(trackedQuery);
    });
  }

  setComplete(query: Query) {
    this.initialized.then(() => {
      query = normalizeQuery(query);
      let trackedQuery = this.find(query);

      if (!trackedQuery) {
        // We might have removed a query and pruned it before we got the complete message from the server
        warn('Trying to set an untracked query as complete');
      } else if (!trackedQuery.complete) {
        trackedQuery.complete = true;
        boundLog(`setComplete id=${trackedQuery.id}`);
        this.trackedQueryStore_.save(trackedQuery);
      }
    });
  }

  setCompletePath(path: Path) {
    boundLog(`setCompletePath path=${path}`);
    this.initialized.then(() => {
      this.trackedQueryTree_
        .subtree(path)
        .foreach((path: Path, map: TrackedQueryMap) => {
          forEach(map, (key: string, trackedQuery: TrackedQuery) => {
            if (!trackedQuery.complete) {
              trackedQuery.complete = true;
              this.trackedQueryStore_.save(trackedQuery);
            }
          });
        });
    });
  }

  isComplete(query: Query): boolean {
    if (this.isIncludedInDefaultCompleteQuery_(query)) {
      return true;
    } else {
      if (query.getQueryParams().loadsAllData()) {
        return false;
      } else {
        const trackedQueries = this.trackedQueryTree_.get(query.path);
        return trackedQueries[query.queryIdentifier()].complete;
      }
    }
  }

  ensureComplete(path: Path) {
    boundLog(`ensureComplete path=${path}`);

    this.initialized.then(() => {
      const query = Query.defaultAtPath(path);

      if (!this.isIncludedInDefaultCompleteQuery_(query)) {
        let trackedQuery = this.find(query);

        if (!trackedQuery) {
          trackedQuery = new TrackedQuery(
            this.nextId++,
            query,
            Date.now(),
            false,
            true
          );
          this.cacheTrackedQuery_(trackedQuery);
        } else {
          assert(
            !trackedQuery.complete,
            'The tracked query was already marked as complete'
          );
          trackedQuery.complete = true;
        }

        this.trackedQueryStore_.save(trackedQuery);
      }
    });
  }

  private isIncludedInDefaultCompleteQuery_(query: Query): boolean {
    return !!this.trackedQueryTree_.findRootMostMatchingPathAndValue(
      query.path,
      (map: TrackedQueryMap) => {
        const trackedQuery = map[Query.DefaultIdentifier];
        return trackedQuery && trackedQuery.complete;
      }
    );
  }

  hasActiveDefault(path: Path): boolean {
    return !!this.trackedQueryTree_.findRootMostMatchingPathAndValue(
      path,
      (map: TrackedQueryMap) => {
        const trackedQuery = map[Query.DefaultIdentifier];
        return trackedQuery && trackedQuery.active;
      }
    );
  }

  knownCompleteChildren(path: Path): Promise<string[]> {
    assert(
      !this.isComplete(Query.defaultAtPath(path)),
      'Path is fully complete'
    );

    const completeChildren: string[] = [];
    const completeBelow: string[] = [];

    // First get the complete children from any queries at this location
    const waitFor: Promise<any>[] = [];
    this.filteredQueries_(path).forEach((trackedQueryId: number) => {
      waitFor.push(
        this.trackedQueryStore_
          .getKeys(trackedQueryId)
          .then((keys: string[]) => {
            boundLog(
              `knownCompleteChildren for id=${trackedQueryId} got`,
              completeChildren.push(...keys),
              'keys'
            );
          })
      );
    });

    // Then get any complete default queries immediately below us
    this.trackedQueryTree_
      .subtree(path)
      .foreachChild((childKey: string, map: TrackedQueryMap) => {
        if (map[Query.DefaultIdentifier].complete) {
          completeBelow.push(childKey);
        }
      });

    return Promise.all(waitFor)
      .then(() => [...completeChildren, ...completeBelow])
      .then((keys: string[]) => {
        boundLog(`knownCompleteChildren path=${path} keys =`, keys);
        return keys;
      });
  }

  private filteredQueries_(path: Path): number[] {
    const trackedQueries = this.trackedQueryTree_.get(path);

    if (!trackedQueries) {
      return [];
    }

    const trackedQueryIds: number[] = [];

    forEach(trackedQueries, (key: string, trackedQuery: TrackedQuery) => {
      if (!trackedQuery.query.getQueryParams().loadsAllData()) {
        trackedQueryIds.push(trackedQuery.id);
      }
    });

    return trackedQueryIds;
  }

  private cacheTrackedQuery_(trackedQuery: TrackedQuery) {
    const query = trackedQuery.query;
    const queryParams = query.getQueryParams();
    assert(
      !queryParams.loadsAllData() || queryParams.isDefault(),
      "Can't have tracked non-default query that loads all data"
    );

    let trackedQueryMap = this.trackedQueryTree_.get(query.path);

    if (!trackedQueryMap) {
      trackedQueryMap = {};
      this.trackedQueryTree_ = this.trackedQueryTree_.set(
        query.path,
        trackedQueryMap
      );
    }

    trackedQueryMap[query.queryIdentifier()] = trackedQuery;
  }

  pruneOld(cachePolicy: CachePolicy): PruneForest {
    const prunableQueries: TrackedQuery[] = [];
    const unprunableQueries: TrackedQuery[] = [];

    this.trackedQueryTree_.foreach((path: Path, map: TrackedQueryMap) => {
      forEach(map, (key: string, trackedQuery: TrackedQuery) => {
        if (trackedQuery.active) {
          prunableQueries.push(trackedQuery);
        } else {
          unprunableQueries.push(trackedQuery);
        }
      });
    });

    prunableQueries.sort(TrackedQuery.lastUseComparator);

    let pruneForest = PruneForest.Empty;
    const numToPrune = numQueriesToPrune(cachePolicy, prunableQueries.length);
    const trackedQueriesToRemove: Query[] = [];

    for (let i = 0; i < numToPrune; i++) {
      const toPrune = prunableQueries[i];
      pruneForest = pruneForest.prunePath(toPrune.query.path);
      this.remove(toPrune.query);
      trackedQueriesToRemove.push(toPrune.query);
    }

    if (trackedQueriesToRemove.length > 0) {
      this.removeBatch_(trackedQueriesToRemove);
    }

    // Keep the rest of the prunable queries
    for (let i = numToPrune; i < prunableQueries.length; i++) {
      const toKeep = prunableQueries[i];
      pruneForest = pruneForest.keepPath(toKeep.query.path);
    }

    // Also keep unprunable queries
    unprunableQueries.forEach((toKeep: TrackedQuery) => {
      pruneForest = pruneForest.keepPath(toKeep.query.path);
    });

    return pruneForest;
  }

  numPrunableQueries(): number {
    let num = 0;

    this.trackedQueryTree_.foreach((path: Path, map: TrackedQueryMap) => {
      forEach(map, (key: string, trackedQuery: TrackedQuery) => {
        if (!trackedQuery.active) {
          num += 1;
        }
      });
    });

    return num;
  }

  private removeBatch_(queries: Query[]) {
    const ids: number[] = [];

    queries.forEach((query: Query) => {
      query = normalizeQuery(query);
      const trackedQuery = this.find(query);

      if (!trackedQuery) {
        warn('Tracked query must exist to be removed');
        return;
      }

      const trackedQueryMap = this.trackedQueryTree_.get(query.path);
      delete trackedQueryMap[query.queryIdentifier()];
      ids.push(trackedQuery.id);
    });

    if (ids.length > 0) {
      this.trackedQueryStore_.removeKeys(ids);
    }
  }

  /*
  // For testing. Not used yet.
  verifyCache() {
    this.trackedQueryStore_.load()
      .then((storedTrackedQueries: TrackedQuery[]) => {
        const trackedQueries: TrackedQuery[] = [];

        this.trackedQueryTree_.foreach((path: Path, map: TrackedQueryMap) => {
          forEach(map, (key: string, trackedQuery: TrackedQuery) => {
            trackedQueries.push(trackedQuery);
          })
        });

        trackedQueries.sort(TrackedQuery.queryIdComparator);
        storedTrackedQueries = storedTrackedQueries.sort(TrackedQuery.queryIdComparator);

        const cacheOK = (trackedQueries.length === storedTrackedQueries.length) &&
          storedTrackedQueries.every((query: TrackedQuery, index: number) => {
            return query.isEqual(trackedQueries[index]);
          });

        assert(cacheOK, 'Tracked queries and persisted queries on storage don\'t match');
      });
  }
  */
}
