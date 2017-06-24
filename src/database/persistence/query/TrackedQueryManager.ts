import { Query } from '../../api/Query';
import { Path } from '../../core/util/Path';
import { ImmutableTree } from '../../core/util/ImmutableTree';
import { TrackedQuery } from './TrackedQuery';
import { assert } from '../../../utils/assert';
import { log, warn } from '../../core/util/util';
import { forEach } from '../../../utils/obj';
import { local } from '../../../app/shared_promise';
import { TrackedQueryStore } from './TrackedQueryStore';

interface TrackedQueryMap {
  [queryId: string]: TrackedQuery;
}

const normalizeQuery = (query: Query) => {
  return query.getQueryParams().loadsAllData()
    ? Query.defaultAtPath(query.path, query.repo)
    : query;
};

const boundLog = log.bind(null, 'TrackedQueryManager:') as (...args: any[]) => void;

export class TrackedQueryManager {
  private nextId = 0;
  private trackedQueryTree_: ImmutableTree<TrackedQueryMap> = ImmutableTree.Empty;

  constructor(private trackedQueryStore_: TrackedQueryStore) {
    this.trackedQueryStore_.load().then((trackedQueries: TrackedQuery[]) => {
      trackedQueries.forEach((trackedQuery: TrackedQuery) => {
        this.nextId = Math.max(trackedQuery.id + 1, this.nextId);
        if (trackedQuery.active) {
          trackedQuery.active = false;
          boundLog(`inactivating query ${trackedQuery.id} from previous app start`);
          this.trackedQueryStore_.save(trackedQuery);
        }
        this.cacheTrackedQuery_(trackedQuery);
      })
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

    this.trackedQueryStore_.removeKeys(trackedQuery.id);
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
    query = normalizeQuery(query);
    let trackedQuery = this.find(query);

    if (trackedQuery) {
      trackedQuery.active = active;
    } else {
      assert(active, 'If we\'re setting the query to inactive, we should already be tracking it');
      trackedQuery = new TrackedQuery(this.nextId++, query, active);
      this.cacheTrackedQuery_(trackedQuery);
    }

    boundLog(`setActiveState id=${trackedQuery.id} active=${active}`);
    this.trackedQueryStore_.save(trackedQuery);
  }

  setComplete(query: Query) {
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
  }

  setCompletePath(path: Path) {
    boundLog(`setCompletePath path=${path}`);
    this.trackedQueryTree_.subtree(path).foreach((path: Path, map: TrackedQueryMap) => {
      forEach(map, (key: string, trackedQuery: TrackedQuery) => {
        if (!trackedQuery.complete) {
          trackedQuery.complete = true;
          this.trackedQueryStore_.save(trackedQuery);
        }
      })
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
    const query = Query.defaultAtPath(path);

    if (!this.isIncludedInDefaultCompleteQuery_(query)) {
      let trackedQuery = this.find(query);

      if (!trackedQuery) {
        trackedQuery = new TrackedQuery(this.nextId++, query, false, true);
        this.cacheTrackedQuery_(trackedQuery);
      } else {
        assert(!trackedQuery.complete, 'The tracked query was already marked as complete');
        trackedQuery.complete = true;
      }

      this.trackedQueryStore_.save(trackedQuery);
    }
  }

  private isIncludedInDefaultCompleteQuery_(query: Query): boolean {
    return !!this.trackedQueryTree_.findRootMostMatchingPathAndValue(query.path,
      (map: TrackedQueryMap) => {
        const trackedQuery = map[Query.DefaultIdentifier];
        return trackedQuery && trackedQuery.complete;
      })
  }

  hasActiveDefault(path: Path): boolean {
    return !!this.trackedQueryTree_.findRootMostMatchingPathAndValue(path,
      (map: TrackedQueryMap) => map[Query.DefaultIdentifier].active);
  }

  knownCompleteChildren(path: Path): Promise<string[]> {
    assert(!this.isComplete(Query.defaultAtPath(path)), 'Path is fully complete');

    const completeChildren: string[] = [];
    const completeBelow: string[] = [];

    // First get the complete children from any queries at this location
    const waitFor: Promise<any>[] = [];
    this.filteredQueries_(path).forEach((trackedQueryId: number) => {
      waitFor.push(this.trackedQueryStore_.getKeys(trackedQueryId)
        .then((keys: string[]) => {
          boundLog(`knownCompleteChildren for id=${trackedQueryId} got`, completeChildren.push(...keys), 'keys');
        }));
    });

    // Then get any complete default queries immediately below us
    this.trackedQueryTree_.subtree(path)
      .foreachChild((childKey: string, map: TrackedQueryMap) => {
        if (map[Query.DefaultIdentifier].complete) {
          completeBelow.push(childKey);
        }
      });

    return local.Promise.all(waitFor)
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
    assert(!queryParams.loadsAllData() || queryParams.isDefault(),
      'Can\'t have tracked non-default query that loads all data');

    let trackedQueryMap = this.trackedQueryTree_.get(query.path);

    if (!trackedQueryMap) {
      trackedQueryMap = {};
      this.trackedQueryTree_ = this.trackedQueryTree_.set(query.path, trackedQueryMap);
    }

    trackedQueryMap[query.queryIdentifier()] = trackedQuery;
  }

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

}
