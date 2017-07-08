import { contains } from '../../../utils/obj';
import { log, warn } from '../../core/util/util';
import { TrackedQuery, TrackedQueryJSON } from './TrackedQuery';
import { Store, QUERIES_STORE_NAME } from '../storage/Store';
import { KeyValueItem, StorageAdapter } from '../storage/StorageAdapter';

// Prefix for the tracked queries entries
const TRACKED_QUERY_PREFIX = 'query/';

// Prefix for the tracked query keys entries
const TRACKED_QUERY_KEY_PREFIX = 'key/';

const storeQueryPrefix = (id: number) => {
  return TRACKED_QUERY_PREFIX + String(id);
};

const storeQueryKeyPrefix = (id: number) => {
  return `${TRACKED_QUERY_KEY_PREFIX}${id}/`;
};

const storeQueryKeysKeyPrefix = (id: number, key: string) => {
  return storeQueryKeyPrefix(id) + key;
};

const boundLog = log.bind(null, 'TrackedQueryStore:') as (...args: any[]) => void;
const boundWarn = warn.bind(null, 'TrackedQueryStore:') as (...args: any[]) => void;

/**
 * Tracked queries store
 */
export class TrackedQueryStore {
  private store_: Store;

  constructor(database: string, storageAdapter: StorageAdapter) {
    this.store_ = new Store(database, QUERIES_STORE_NAME, storageAdapter);
  }

  close() {
    return this.store_.close().then(() => boundLog(`closed`));
  }

  load(): Promise<TrackedQuery[]> {
    const result: TrackedQuery[] = [];

    return this.store_.getAll(TRACKED_QUERY_PREFIX)
      .then((trackedQueries: KeyValueItem<TrackedQueryJSON>[]) => {
        const writeBatch = this.store_.writeBatch();
        let removeKeysCount = 0;

        trackedQueries.forEach((item: KeyValueItem<TrackedQueryJSON>) => {
          let trackedQuery;

          try {
            trackedQuery = TrackedQuery.fromJSON(item.value);
          } catch (e) { }

          if (!trackedQuery) {
            boundWarn(`failed to restore tracked query key="${item.key}". Removing it.`);
            writeBatch.remove(item.key);
            removeKeysCount += 1;
          } else {
            result.push(trackedQuery);
          }
        });

        if (removeKeysCount > 0) {
          writeBatch.run().catch((error: Error) => {
            boundWarn('failed to remove query keys while loading.', error);
          });
        }

        boundLog(`loaded ${result.length} tracked queries`);
        return result;
      })
      .catch((error: Error) => {
        boundWarn(`failed to load tracked queries`, error);
        return [];
      });
  }

  save(trackedQuery: TrackedQuery) {
    this.store_.set(storeQueryPrefix(trackedQuery.id), trackedQuery.toJSON())
      .then(() => {
        boundLog(`saved id=${trackedQuery.id}`);
      })
      .catch((error: Error) => {
        boundWarn(`failed to save id=${trackedQuery.id}`, error);
      });
  }

  getKeys(id: number): Promise<string[]> {
    return this.store_.getAll(storeQueryKeyPrefix(id))
      .then((trackedQueryKeys: KeyValueItem<string>[]) => {
        boundLog(`got ${trackedQueryKeys.length} tracked keys id=${id}`);
        return trackedQueryKeys.map((item: KeyValueItem<string>) => item.value);
      })
      .catch((error: Error) => {
        boundWarn(`failed to load keys id=${id}`, error);
        return [];
      });
  }

  setKeys(id: number, keys: string[]) {
    const writeBatch = this.store_.writeBatch();
    const seenKeys: { [k: string]: boolean } = {};
    let added = 0;
    let removed = 0;

    // Delete any keys that might be stored and are not part of the current keys
    this.store_.getAll(storeQueryKeyPrefix(id))
      .then((trackedQueryKeys: KeyValueItem<string>[]) => {
        trackedQueryKeys.forEach((trackedQueryKey: KeyValueItem<string>) => {
          if (contains(keys, trackedQueryKey.value)) {
            // Already in DB
            seenKeys[trackedQueryKey.value] = true;
          } else {
            // Not part of set, delete key
            writeBatch.remove(trackedQueryKey.key);
            removed += 1;
          }
        });
      })
      .then(() => {
        // Add any keys that are missing in the database
        keys.forEach((childKey: string) => {
          if (!contains(seenKeys, childKey)) {
            writeBatch.set(storeQueryKeysKeyPrefix(id, childKey), childKey);
            added += 1;
          }
        });

        if (added + removed > 0) {
          return writeBatch.run();
        }
      })
      .then(() => {
        if (added + removed > 0) {
          boundLog(`setKeys added=${added} removed=${removed} tracked keys id=${id}`);
        }
      })
      .catch((error: Error) => {
        boundWarn(`setKeys failed id=${id}`, error);
      });
  }

  updateKeys(id: number, addedKeys: string[], removedKeys: string[]) {
    const writeBatch = this.store_.writeBatch();

    removedKeys.forEach((key: string) => writeBatch.remove(storeQueryKeysKeyPrefix(id, key)));
    addedKeys.forEach((key: string) => writeBatch.set(storeQueryKeysKeyPrefix(id, key), key));

    writeBatch.run()
      .then(() => {
        boundLog(`updateKeys added=${addedKeys.length} removed=${removedKeys.length} tracked keys id=${id}`);
      })
      .catch((error: Error) => {
        boundWarn(`updateKeys failed id=${id}`, error);
      });
  }

  removeKeys(ids: number[]) {
    ids.forEach((id: number) => {
      const writeBatch = this.store_.writeBatch();

      writeBatch.remove(storeQueryPrefix(id));

      this.store_.keys(storeQueryKeyPrefix(id))
        .then((keys: string[]) => {
          writeBatch.remove(keys);
          return writeBatch.run().then(() => keys.length);
        })
        .then((numKeys: number) => {
          boundLog(`removed tracked query id=${id} and ${numKeys} tracked keys`);
        })
        .catch((error: Error) => {
          boundWarn(`removeKeys failed id=${id}`, error);
        });
    });
  }

}
