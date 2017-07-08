import { assert } from '../../utils/assert';
import { forEach } from '../../utils/obj';
import { PromiseImpl } from '../../utils/promise';
import { Repo } from '../core/Repo';
import { log, warn } from '../core/util/util';
import { Path } from '../core/util/Path';
import { Node } from '../core/snap/Node';
import { Query } from '../api/Query';
import { CacheNode } from '../core/view/CacheNode';
import { StorageAdapter } from './storage/StorageAdapter';
import { TrackedQueryManager } from './query/TrackedQueryManager';
import { ServerCacheStore } from './ServerCacheStore';
import { PersistedUserWrite, UserWriteStore } from './UserWriteStore';
import { TrackedQueryStore } from './query/TrackedQueryStore';
import { IDBStorageAdapter } from './storage/IDBStorageAdapter';
import { ChildrenNode } from '../core/snap/ChildrenNode';
import { CachePolicy } from './cache/CachePolicy';

// Prefix for the database name to use
const DATABASE_PREFIX = 'firebase:';

const boundLog = log.bind(null, 'PersistenceManager:') as (...args: any[]) => void;
const boundWarn = warn.bind(null, 'PersistenceManager:') as (...args: any[]) => void;

/**
 * Persistence manager for a Repo instance
 */
export class PersistenceManager {
  private trackedQueryManager_: TrackedQueryManager;
  private serverUpdatesSincePruneCheck_ = 0;

  private serverCacheStore_: ServerCacheStore;
  private userWriteStore_: UserWriteStore;
  private trackedQueryStore_: TrackedQueryStore;

  constructor(repo: Repo, private cachePolicy_: CachePolicy,
              storageAdapter?: StorageAdapter | null) {
    const database = DATABASE_PREFIX + repo.app.options.apiKey;

    if (!storageAdapter) {
      storageAdapter = new IDBStorageAdapter();
    }

    this.serverCacheStore_ = new ServerCacheStore(database, storageAdapter);
    this.userWriteStore_ = new UserWriteStore(database, storageAdapter);
    this.trackedQueryStore_ = new TrackedQueryStore(database, storageAdapter);
    this.trackedQueryManager_ = new TrackedQueryManager(this.trackedQueryStore_);
  }

  /**
   * Close this persistence manager. It won't be used anymore.
   */
  close(): Promise<any> {
    return PromiseImpl.all([
      this.serverCacheStore_.close(),
      this.userWriteStore_.close(),
      this.trackedQueryStore_.close()
    ]).then(() => boundLog('all closed'));
  }

  /* ======= [BEGIN] USER WRITES ======= */

  getUserWrites(): Promise<PersistedUserWrite[]> {
    return this.userWriteStore_.getAll();
  }

  saveUserOverwrite(path: Path, newData: Node, writeId: number) {
    this.userWriteStore_.overwrite(path, newData, writeId);
  }

  saveUserMerge(path: Path, changedChildren: { [k: string]: Node }, writeId: number) {
    this.userWriteStore_.merge(path, changedChildren, writeId);
  }

  removeUserWrite(writeId: number) {
    this.userWriteStore_.remove(writeId);
  }

  applyUserMerge(merge: { [p: string]: Node }, path: Path) {
    boundLog(`applyUserMerge path=${path}`);
    forEach(merge, (childKey: Path, childNode: Node) => {
      this.applyUserWrite(childNode, path.child(childKey));
    });
  }

  applyUserWrite(node: Node, path: Path) {
    if (this.trackedQueryManager_.hasActiveDefault(path)) {
      boundLog(`applyUserWrite path=${path}`);
      this.serverCacheStore_.overwrite(node, path, false)
        .then(() => {
          this.trackedQueryManager_.ensureComplete(path);
        });
    }
  }

  /*
  // For tests. Not used yet.
  clearUserWrites() {
    this.userWriteStore_.clear();
  }
  */

  /* ======= [END] USER WRITES ======= */

  /* ======= [BEGIN] SERVER CACHE ======= */

  getServerCache(query: Query): Promise<CacheNode> {
    let keysPromise: Promise<string[]>;
    let complete: boolean;

    if (this.trackedQueryManager_.isComplete(query)) {
      complete = true;
      const trackedQuery = this.trackedQueryManager_.find(query);
      boundLog(`getServerCache for complete query id=${trackedQuery.id}`);

      if (!query.getQueryParams().loadsAllData() && trackedQuery.complete) {
        keysPromise = this.trackedQueryStore_.getKeys(trackedQuery.id);
      }
    } else {
      boundLog(`getServerCache for incomplete query path=${query.path}`);
      complete = false;
      keysPromise = this.trackedQueryManager_.knownCompleteChildren(query.path);
    }

    let nodePromise: Promise<Node>;

    if (keysPromise) {
      nodePromise = keysPromise.then((keys: string[]) => this.serverCacheStore_.getForKeys(keys, query.path));
    } else {
      nodePromise = this.serverCacheStore_.getAtPath(query.path);
    }

    return nodePromise
      .then((node: Node) => new CacheNode(node, complete, !!keysPromise))
      .catch((error: Error) => {
        boundWarn('failed loading server cache.', error);
        // Let's make sure to return an empty CacheNode
        return new CacheNode(ChildrenNode.EMPTY_NODE, false, false);
      });
  }

  applyServerOverwrite(node: Node, query: Query) {
    const partial = !query.getQueryParams().loadsAllData();

    this.serverCacheStore_.overwrite(node, query.path, partial)
      .then(() => {
        this.setQueryComplete(query);
        this.pruneCheck_();
      });
  }

  applyServerMerge(merge: { [p: string]: Node }, path: Path) {
    this.serverCacheStore_.merge(merge, path).then(() => {
      this.pruneCheck_();
    });
  }

  private pruneCheck_() {
    this.serverUpdatesSincePruneCheck_ += 1;

    if (this.cachePolicy_.shouldCheckSize(this.serverUpdatesSincePruneCheck_)) {
      boundLog(`reached prune check threshold after ${this.serverUpdatesSincePruneCheck_} updates. Checking...`);
      this.serverUpdatesSincePruneCheck_ = 0;
      this.recursivePruneCheck_()
        .then(() => {
          boundLog(`finished pruning server cache`);
        })
        .catch((error: Error) => {
          boundWarn(`pruning server cache failed`, error);
        });
    }
  }

  private recursivePruneCheck_(isAfterPruning = false): Promise<void> {
    return this.serverCacheStore_.estimatedSize().then((cacheSize: number) => {
      boundLog(`cache size${isAfterPruning ? ' after pruning' : ''}: ${cacheSize}`);

      if (this.cachePolicy_.shouldPrune(cacheSize, this.trackedQueryManager_.numPrunableQueries())) {
        const pruneForest = this.trackedQueryManager_.pruneOld(this.cachePolicy_);
        if (pruneForest.prunesAnything()) {
          return this.serverCacheStore_.pruneCache(pruneForest, Path.Empty)
            .then(() => this.recursivePruneCheck_(true));
        }
      }
    });
  }

  /* ======= [END] SERVER CACHE ======= */

  /* ======= [BEGIN] TRACKED QUERIES ======= */

  setQueryComplete(query: Query) {
    const loadsAllData = query.getQueryParams().loadsAllData();
    boundLog(`setQueryComplete loadsAllData=${loadsAllData}`);
    if (loadsAllData) {
      this.trackedQueryManager_.setCompletePath(query.path);
    } else {
      this.trackedQueryManager_.setComplete(query);
    }
  }

  setQueryActive(query: Query) {
    this.trackedQueryManager_.setActive(query);
  }

  setQueryInactive(query: Query) {
    this.trackedQueryManager_.setInactive(query);
  }

  setTrackedQueryKeys(query: Query, keys: string[]) {
    assert(!query.getQueryParams().loadsAllData(), 'We should only track keys for filtered queries');
    const trackedQuery = this.trackedQueryManager_.find(query);
    assert(trackedQuery.active, 'We only expect tracked keys for currently-active queries');
    this.trackedQueryStore_.setKeys(trackedQuery.id, keys);
  }

  updateTrackedQueryKeys(query: Query, addedKeys: string[], removedKeys: string[]) {
    assert(!query.getQueryParams().loadsAllData(), 'We should only track keys for filtered queries');
    const trackedQuery = this.trackedQueryManager_.find(query);
    assert(trackedQuery.active, 'We only expect tracked keys for currently-active queries');
    this.trackedQueryStore_.updateKeys(trackedQuery.id, addedKeys, removedKeys);
  }

  /* ======= [END] TRACKED QUERIES ======= */
}
