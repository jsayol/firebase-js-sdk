import { StorageAdapter, StorageAdapterWriteBatch } from './StorageAdapter';
import { QUERIES_STORE_NAME, SERVER_STORE_NAME, USER_STORE_NAME, VALID_STORES } from './Store';
import { contains, every } from '../../../utils/obj';
import { assert } from '../../../utils/assert';

/**
 * WARNING: If this version number is changed, make sure to include the necessary migration script
 * inside the `onupgradeneeded` event in the `getDatabase_()` method.
 * @type {number}
 * @const
 */
const DATABASE_VERSION = 1;

/**
 * A reference to the detected IndexedDB implementation, if any;
 * @type {IDBFactory}
 */
let idbFactoryImpl: IDBFactory;

/**
 * Provides an implementation of StorageAdapter using IndexedDB
 *
 * The "Promisification" of IndexedDB has been heavily inspired by the
 * `idb-keyval` package by Jake Archibald: https://github.com/jakearchibald/idb-keyval
 *
 * TODO(jsayol): Safari is known to have very buggy IndexedDB support. Check for potential errors.
 *
 * TODO(jsayol): maybe each "store" should be a separate IndexedDB database, instead of
 * object stores inside the same database. Look into it.
 *
 * @implements {StorageAdapter}
 */
export class IDBStorageAdapter implements StorageAdapter {
  /** @inheritDoc */
  writeThrottleTime = 1000;

  /**
   * An object of promises that resolve with the database object
   * @type {Promise<IDBDatabase>}
   * @private
   */
  private db_: { [name: string]: Promise<IDBDatabase> } = {};

  private stores_: {
    [db: string]: {
      [store: string]: { open: boolean }
    }
  } = {};

  constructor() {
    validateIndexedDBSupport();
  }

  /** @inheritDoc */
  get(dbName: string, storeName: string, key: string): Promise<any> {
    let req: IDBRequest;

    return this.runTransaction(dbName, storeName, 'readonly', (store: IDBObjectStore) => {
      req = store.get(key);
    }).then(() => req.result);
  }

  /** @inheritDoc */
  getAll(dbName: string, storeName: string, prefix?: string): Promise<{ key: string, value: any }[]> {
    const pairs: { key: string, value: any }[] = [];

    // If we're using a prefix then build the corresponding IDBKeyRange
    const keyRange = generatePrefixKeyRange(prefix);

    return this.runTransaction(dbName, storeName, 'readonly', (store: IDBObjectStore) => {
      const cursorReq = store.openCursor(keyRange);

      cursorReq.onsuccess = () => {
        const cursor = <IDBCursorWithValue>cursorReq.result;
        if (!cursor) {
          return;
        }

        pairs.push({
          key: <string>cursor.key,
          value: cursor.value
        });

        cursor.continue();
      };
    }).then(() => pairs);
  }

  /** @inheritDoc */
  set(dbName: string, storeName: string, key: string, value?: any): Promise<any> {
    const writeBatch = this.writeBatch(dbName, storeName);
    writeBatch.set(key, value);
    return writeBatch.run();
  }

  /** @inheritDoc */
  remove(dbName: string, storeName: string, key: string): Promise<any> {
    const writeBatch = this.writeBatch(dbName, storeName);
    writeBatch.remove(key);
    return writeBatch.run();
  }

  /** @inheritDoc */
  removePrefixed(dbName: string, storeName: string, prefix: string): Promise<any> {
    const writeBatch = this.writeBatch(dbName, storeName);
    writeBatch.removePrefixed(prefix);
    return writeBatch.run();
  }

  /** @inheritDoc */
  clear(dbName: string, storeName: string): Promise<any> {
    return this.runTransaction(dbName, storeName, 'readwrite', (store: IDBObjectStore) => {
      store.clear();
    });
  }

  /** @inheritDoc */
  keys(dbName: string, storeName: string, prefix?: string): Promise<string[]> {
    const keys: string[] = [];

    // If we're using a prefix then build the corresponding IDBKeyRange
    const keyRange = generatePrefixKeyRange(prefix);

    return this.runTransaction(dbName, storeName, 'readonly', (store: IDBObjectStore) => {
      // This would be store.getAllKeys(), but it isn't supported by Edge or Safari.
      // And openKeyCursor isn't supported by Safari.
      const cursorReq: IDBRequest = ((<any>store).openKeyCursor || store.openCursor).call(store, keyRange);

      cursorReq.onsuccess = () => {
        const cursor = <IDBCursor>cursorReq.result;
        if (!cursor) {
          return;
        }
        keys.push(<string>cursor.key);
        cursor.continue();
      };
    }).then(() => keys);
  }

  /** @inheritDoc */
  count(dbName: string, storeName: string): Promise<number> {
    let count: number;

    return this.runTransaction(dbName, storeName, 'readonly', (store: IDBObjectStore) => {
      const countReq: IDBRequest = store.count();
      countReq.onsuccess = function () {
        count = <number>this.result || 0;
      };
    }).then(() => count);
  }

  /** @inheritDoc */
  close(dbName: string, storeName: string): Promise<any> {
    assert(contains(this.stores_, dbName), `There is no database named '${dbName}'`);
    assert(contains(this.stores_[dbName], storeName), `There is no store named '${storeName}' in database '${dbName}'`);
    assert(this.stores_[dbName][storeName].open, `There '${storeName}' store in database '${dbName}' was already closed`);

    this.stores_[dbName][storeName].open = false;

    if (!every(this.stores_[dbName], (name: string, store: { open: boolean }) => !store.open)) {
      // There's still other open stores so don't do anything else
      return Promise.resolve();
    }

    // All stores have been closed, let's close the database
    return this.getDatabase_(dbName).then((db: IDBDatabase) => {
      db.close();
    });
  }

  /** @inheritDoc */
  writeBatch(dbName: string, storeName: string): IDBWriteBatch {
    return new IDBWriteBatch(this, dbName, storeName);
  }

  /** @inheritDoc */
  pruneCheck(dbName: string, storeName: string) {
    // Each browser's IndexedDB implementation handles its own quotas
    // and any necessary pruning. No need to do anything here.
  }

  /**
   * Creates a new IndexedDB transaction and executes the given callback
   * with the store object.
   *
   * This method should be private but it's also used by IDBWriteBatch
   *
   * @param dbName
   * @param {string} storeName
   * @param {!IDBTransactionMode} type
   * @param {!function(!IDBObjectStore)} callback
   * @return {Promise<void>} A promise that resolve once the transaction has completed
   */
  runTransaction(dbName: string, storeName: string, type: IDBTransactionMode,
                 callback: (store: IDBObjectStore) => void): Promise<void> {
    return this.getDatabase_(dbName).then((db: IDBDatabase) => {
      return new Promise<void>((resolve: () => void, reject: (reason?: any) => void) => {
        const transaction = db.transaction(storeName, type);
        transaction.oncomplete = () => resolve();
        transaction.onerror = () => reject(transaction.error);
        callback(transaction.objectStore(storeName));
      });
    });
  }

  /**
   * Opens the database and handles any necessary upgrading
   *
   * @return {Promise<IDBDatabase>} A promise that resolve with the database when it's available
   * @private
   */
  private getDatabase_(dbName: string): Promise<IDBDatabase> {
    if (!contains(this.db_, dbName)) {
      this.db_[dbName] = new Promise((resolve: (value: IDBDatabase) => void, reject: (reason?: any) => void) => {
        const openreq = idbFactoryImpl.open(dbName, DATABASE_VERSION);

        openreq.onerror = () => reject(openreq.error);

        openreq.onsuccess = () => {
          VALID_STORES.forEach((storeName: string) => {
            const dbStores = this.stores_[dbName] || (this.stores_[dbName] = {});
            dbStores[storeName] = {open: true};
          });
          resolve(openreq.result);
        };

        openreq.onupgradeneeded = (event: IDBVersionChangeEvent) => {
          // We don't use 'break' in this switch statement,
          // the fall-through behaviour is what we want.
          switch (event.oldVersion) {
            case 0:
              // First time setup: create the necessary empty object stores
              (<IDBDatabase>openreq.result).createObjectStore(SERVER_STORE_NAME);
              (<IDBDatabase>openreq.result).createObjectStore(USER_STORE_NAME);
              (<IDBDatabase>openreq.result).createObjectStore(QUERIES_STORE_NAME);
            case 1:
            // Note: when upgrading the database version from 1 to 2, put
            // any necessary migration script here.
          }
        };
      });
    }

    return this.db_[dbName];
  }

}

/**
 * An implementation of StorageAdapterWriteBatch using IndexedDB
 *
 * @implements {StorageAdapterWriteBatch}
 */
export class IDBWriteBatch implements StorageAdapterWriteBatch {
  private operations_: Array<(store: IDBObjectStore) => void> = [];

  constructor(private storageAdapter_: IDBStorageAdapter,
              private dbName_: string,
              private storeName_: string) {
  }

  set(key: string, value?: any) {
    this.operations_.push((store: IDBObjectStore) => {
      if ((value === void 0) || (value === null)) {
        store.delete(key);
      } else {
        store.put(value, key);
      }
    });
  }

  remove(key: string) {
    this.operations_.push((store: IDBObjectStore) => {
      store.delete(key);
    });
  }

  removePrefixed(prefix: string) {
    this.operations_.push((store: IDBObjectStore) => {
      const keyRange = generatePrefixKeyRange(prefix);
      store.delete(keyRange);
    });
  }

  run(): Promise<any> {
    assert(this.operations_.length > 0, 'Cannot execute a write batch with no operations');

    return this.storageAdapter_.runTransaction(this.dbName_, this.storeName_, 'readwrite', (store: IDBObjectStore) => {
      const operations = this.operations_;
      this.operations_ = [];
      operations.forEach((fn: Function) => fn(store));
    });
  }

}


/**
 * Validates that the platform has IndexedDB support
 */
const validateIndexedDBSupport = () => {
  if (idbFactoryImpl) {
    // We already detected a valid IndexedDB implementation
    return;
  }

  // Try to use any available implementation
  const availableIDB: IDBFactory = self['indexedDB']
    || self['mozIndexedDB']
    || self['webkitIndexedDB']
    || self['msIndexedDB'];

  const isSupported = (typeof availableIDB === 'object') && (typeof availableIDB.open === 'function');
  if (!isSupported) {
    throw new Error('NO_INDEXEDDB_SUPPORT');
  }

  // The detected IndexedDB implementation seems to be valid
  idbFactoryImpl = availableIDB;

  // Some older-ish browsers have several IDB* objects behind a prefix
  self['IDBKeyRange'] = self['IDBKeyRange'] || self['webkitIDBKeyRange'] || self['msIDBKeyRange'];
  self['IDBTransaction'] = self['IDBTransaction'] || self['webkitIDBTransaction']
    || self['msIDBTransaction'] || {READ_WRITE: 'readwrite'};
};

const generatePrefixKeyRange = (prefix: string) => {
  return (typeof prefix === 'string')
    ? IDBKeyRange.bound(prefix, prefix + '\uffff', false, false)
    : void 0;
};