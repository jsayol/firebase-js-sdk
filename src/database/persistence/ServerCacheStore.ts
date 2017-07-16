import { assert } from '../../utils/assert';
import { forEach } from '../../utils/obj';
import { PromiseImpl } from '../../utils/promise';
import { log, warn } from '../core/util/util';
import { Path } from '../core/util/Path';
import { Node } from '../core/snap/Node';
import { nodeFromJSON } from '../core/snap/nodeFromJSON';
import { ChildrenNode } from '../core/snap/ChildrenNode';
import { KEY_INDEX } from '../core/snap/indexes/KeyIndex';
import { Store, SERVER_STORE_NAME } from './storage/Store';
import { KeyValueItem, StorageAdapter, StorageAdapterWriteBatch } from './storage/StorageAdapter';
import { PruneForest } from './cache/PruneForest';

// Generates a server cache key based on a path
const storeKey = (path: Path): string => {
  return path.toString(true);
};

const boundLog = log.bind(null, 'ServerCacheStore:') as (...args: any[]) => void;
const boundWarn = warn.bind(null, 'ServerCacheStore:') as (...args: any[]) => void;

/**
 * Server cache store
 */
export class ServerCacheStore {
  private store_: Store;

  constructor(database: string, storageAdapter: StorageAdapter) {
    this.store_ = new Store(database, SERVER_STORE_NAME, storageAdapter);
  }

  close() {
    return this.store_.close().then(() => boundLog(`closed`));
  }

  overwrite(node: Node, path: Path, partial: boolean): Promise<any> {
    // Create a new write batch on the store
    const writeBatch = this.store_.writeBatch();

    // Remove any leaf nodes that might be higher up
    ServerCacheStore.removeLeafNodes_(path, writeBatch);

    let numLeafs = 0;

    if (partial) {
      // Remove all the children that will be overwritten
      node.forEachChild(KEY_INDEX, (childKey: string, childNode: Node) => {
        const childPath = path.child(childKey);
        writeBatch.removePrefixed(storeKey(childPath));
        numLeafs += ServerCacheStore.saveNode_(childNode, childPath, writeBatch);
      });
    } else {
      // Remove everything at the node's path
      writeBatch.removePrefixed(storeKey(path));
      numLeafs = ServerCacheStore.saveNode_(node, path, writeBatch);
    }

    // Run the write batch
    return writeBatch.run()
      .then(() => {
        boundLog(`overwrite saved ${numLeafs} leaf nodes path=${path} partial=${partial}`);
      })
      .catch((error: Error) => {
        boundWarn(`overwrite failed path=${path} partial=${partial}`, error);
      });
  }

  merge(merge: { [p: string]: Node }, path: Path): Promise<any> {
    // Create a new write batch on the store
    const writeBatch = this.store_.writeBatch();

    // Remove any leaf nodes that might be higher up
    ServerCacheStore.removeLeafNodes_(path, writeBatch);

    let numLeafs = 0;
    forEach(merge, (childKey: Path, childNode: Node) => {
      const childPath = path.child(childKey);
      writeBatch.removePrefixed(storeKey(childPath));
      numLeafs += ServerCacheStore.saveNode_(childNode, childPath, writeBatch);
    });

    // Run the write batch
    return writeBatch.run()
      .then(() => {
        boundLog(`merge saved ${numLeafs} leaf nodes path=${path}`);
      })
      .catch((error: Error) => {
        boundWarn(`merge failed path=${path}`, error);
      });
  }

  getAtPath(path: Path, logging = false): Promise<Node> {
    assert(path, 'Can\'t get server cache without a path');
    const baseKey = storeKey(path);

    return this.store_.getAll<any>(baseKey)
      .then((items: KeyValueItem<any>[]) => {
        let data: any;

        if (items.length === 0) {
          // No data found for that path
          data = null;
        } else {
          if ((items.length === 1) && (baseKey === items[0].key)) {
            // A single value for the key we were looking for
            data = items[0].value;
          } else {
            data = {};

            items.forEach((item: KeyValueItem<any>) => {
              let child = data;
              const subpath = item.key.substring(baseKey.length)
                .split('/').filter(part => part.length > 0);

              const pathLength = subpath.length - 1;
              for (let i = 0; i < pathLength; i++) {
                child[subpath[i]] = child[subpath[i]] || {};
                child = child[subpath[i]];
              }
              child[subpath[pathLength]] = item.value;
            });
          }
        }

        return nodeFromJSON(data);
      })
      .then((node: Node) => {
        if (logging) {
          boundLog(`getAtPath loaded ${node.numChildren()} children path=${path}`);
        }
        return node;
      })
      .catch((error: Error) => {
        boundWarn(`getAtPath failed path=${path}`, error);
        // Rethrow the error to notify the caller
        throw error;
      });
  }

  getForKeys(keys: string[], path: Path): Promise<Node> {
    let node = ChildrenNode.EMPTY_NODE as Node;
    const waitFor: Promise<void>[] = [];

    keys.forEach((key: string) => {
      const cachePromise = this.getAtPath(path.child(key), false)
        .then((childNode: Node) => {
          node = node.updateImmediateChild(key, childNode);
        });
      waitFor.push(cachePromise);
    });

    return PromiseImpl.all(waitFor)
      .then(() => {
        boundLog(`getForKeys loaded ${node.numChildren()} children path=${path}`);
        return node;
      })
      .catch((error: Error) => {
        boundWarn(`getForKeys failed path=${path}`, error);
        // Rethrow the error to notify the caller
        throw error;
      });
  }

  /**
   * Prunes the server cache based on a PruneForest and a Path
   */
  pruneCache(pruneForest: PruneForest, path: Path): Promise<void> {
    let pruned = 0;
    let kept = 0;
    const prefix = storeKey(path);

    return this.store_.keys(prefix).then((keys: string[]) => {
      const writeBatch = this.store_.writeBatch();

      keys.forEach((key: string) => {
        const relativePath = new Path(key.substring(prefix.length));
        if (pruneForest.shouldPruneUnkeptDescendants(relativePath)) {
          pruned += 1;
          writeBatch.remove(key);
        } else {
          kept += 1;
        }
      });

      if ((pruned === 0) && (kept === 0)) {
        return;
      }

      return writeBatch.run()
        .then(() => {
          boundLog(`pruned ${pruned} paths, kept ${kept} paths path=${path}`);
        })
        .catch((error: Error) => {
          boundWarn(`pruneCache failed path=${path}`, error);
        });
    });
  }

  estimatedSize(): Promise<number> {
    return this.store_.estimatedSize();
  }

  private static removeLeafNodes_(path: Path, writeBatch: StorageAdapterWriteBatch) {
    while (!path.isEmpty()) {
      writeBatch.remove(storeKey(path));
      path = path.parent();
    }

    // Make sure to delete any nodes at the root
    writeBatch.remove(storeKey(Path.Empty));
  }

  private static saveNode_(node: Node, path: Path, writeBatch: StorageAdapterWriteBatch): number {
    const data = node.val(true);
    if (data !== null) {
      return ServerCacheStore.setNestedData_(data, storeKey(path), writeBatch);
    }
    return 0;
  }

  private static setNestedData_(data: any, key: string, writeBatch: StorageAdapterWriteBatch): number {
    if ((typeof data === 'object') && !Array.isArray(data)) {
      let numLeafs = 0;
      forEach(data, (childKey: string, childVal: any) => {
        const childPathStr = key + childKey + '/';
        numLeafs += ServerCacheStore.setNestedData_(childVal, childPathStr, writeBatch);
      });
      return numLeafs;
    } else {
      writeBatch.set(key, data);
      return 1;
    }
  }

}
