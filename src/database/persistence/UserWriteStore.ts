import { log, warn } from '../core/util/util';
import { Path } from '../core/util/Path';
import { Node } from '../core/snap/Node';
import { Store, USER_STORE_NAME } from './storage/Store';
import { KeyValueItem, StorageAdapter } from './storage/StorageAdapter';
import { forEach } from '../../utils/obj';

// Generates a user write key based on a writeId
const storeKey = (writeId: number): string => {
  return String(writeId);
};

const boundLog = log.bind(null, 'UserWriteStore:') as (...args: any[]) => void;
const boundWarn = warn.bind(null, 'UserWriteStore:') as (...args: any[]) => void;

export interface PersistedUserWrite {
  id: number;
  path: string;
  overwrite?: any;
  merge?: { [p: string]: any };
}

/**
 * User writes store
 */
export class UserWriteStore {
  private store_: Store;

  constructor(database: string, storageAdapter: StorageAdapter) {
    this.store_ = new Store(database, USER_STORE_NAME, storageAdapter);
  }

  close() {
    return this.store_.close().then(() => boundLog(`closed`));
  }

  overwrite(path: Path, newData: Node, writeId: number) {
    const userWrite: PersistedUserWrite = {
      id: writeId,
      path: path.toString(),
      overwrite: newData.val(true)
    };

    this.store_.set(storeKey(writeId), userWrite)
      .then(() => {
        boundLog(`overwrite saved writeId=${writeId} path=${path}`);
      })
      .catch((error: Error) => {
        boundWarn(`failed to save user overwrite writeId=${writeId} path=${path}`, error);
      });
  }

  merge(path: Path, changedChildren: { [k: string]: Node }, writeId: number) {
    const merge: { [p: string]: any } = {};
    forEach(changedChildren, (childPath: string, child: Node) => merge[childPath] = child.val(true));

    const userWrite: PersistedUserWrite = {
      id: writeId,
      path: path.toString(),
      merge
    };

    this.store_.set(storeKey(writeId), userWrite)
      .then(() => {
        boundLog(`merge saved writeId=${writeId} path=${path}`);
      })
      .catch((error: Error) => {
        boundWarn(`failed to save user merge writeId=${writeId} path=${path}`, error);
      });
  }

  remove(writeId: number) {
    this.store_.remove(storeKey(writeId))
      .then(() => {
        boundLog(`removed writeId=${writeId}`);
      })
      .catch((error: Error) => {
        boundWarn(`failed to remove writeId=${writeId}`, error);
      });
  }

  clear() {
    this.store_.clear()
      .then(() => {
        boundLog(`cleared`);
      })
      .catch((error: Error) => {
        boundWarn(`failed to clear`, error);
      });
  }

  getAll(): Promise<PersistedUserWrite[]> {
    return this.store_.getAll()
      .then((items: KeyValueItem<PersistedUserWrite>[]) => {
        boundLog(`getAll loaded ${items.length} items`);
        return items
          .map((item: KeyValueItem<PersistedUserWrite>) => item.value)
          .sort((w1, w2) => w1.id < w2.id ? -1 : 1);
      })
      .catch((error: Error) => {
        boundWarn(`getAll failed`, error);
      });
  }

}
