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
 * Defines a common interface for a storage adapter to be used for data persistence.
 *
 * A StorageAdapter simply provides a way to manage a key-value store. Any persistence-specific
 * logic is handled by PersistenceManager.
 *
 * The consumer might not wait for the storage adapter to be properly initialized before it
 * starts issuing operations on it, so it's the responsibility of the StorageAdapter implementation
 * to ensure that it's been initialized at any given point.
 */
export interface StorageAdapter {
  /**
   * Indicates how often at most to execute a write operation for any particular path.
   * If it's 0 or an otherwise falsey value then no throttling is performed.
   *
   * TODO(jsayol): throttling is not implemented yet.
   *
   * @type {number=} Time in miliseconds
   */
  readonly writeThrottleTime?: number | null;

  /**
   * The maximum size in bytes for the server cache.
   * If it's undefined or otherwise falsy then the default value will be used.
   *
   * @type {number=} Size in bytes
   */
  readonly maxServerCacheSize?: number | null;

  /**
   * Retrieves from storage the value associated with a given key
   *
   * @param {!string} database The name of the database to use
   * @param {!string} store The name of the store to use
   * @param {!string} key The key name for the value to retrieve
   * @return {!Promise<*>} A promise that resolves with the stored value, or null if not found
   */
  get(database: string, store: string, key: string): Promise<any>;

  /**
   * Retrieves from storage all the key-value pairs. If a prefix is given, then only
   * the pairs whose key begins with that prefix are returned.
   *
   * @param {!string} database The name of the database to use
   * @param {!string} store The name of the store to use
   * @param {string=} prefix The optional prefix to use
   * @return {!Promise<!Object>} A promise that resolves with an array of key-value objects
   */
  getAll<T>(
    database: string,
    store: string,
    prefix?: string
  ): Promise<KeyValueItem<T>[]>;

  /**
   * Stores a value associated with a given key.
   * If called with a null or undefined value then it's equivalent to performing a `remove()` operation.
   *
   * @param {!string} database The name of the database to use
   * @param {!string} store The name of the store to use
   * @param {!string} key The key name for the value to store
   * @param {?*=} value The value to store
   * @return {!Promise<*>} A promise that resolves when the operation has finished successfully
   */
  set(database: string, store: string, key: string, value?: any): Promise<any>;

  /**
   * Removes one or more entries from storage given their keys.
   *
   * @param {!string} database The name of the database to use
   * @param {!string} store The name of the store to use
   * @param {!string|Array.<!string>} keys The key name or array of key name for the values to remove
   * @return {!Promise<*>} A promise that resolves when the operation has finished successfully
   */
  remove(
    database: string,
    store: string,
    keys: string | string[]
  ): Promise<any>;

  /**
   * Removes any entry whose key begins with the given prefix.
   *
   * @param {!string} database The name of the database to use
   * @param {!string} store The name of the store to use
   * @param {!string} prefix The prefix to use
   * @return {!Promise<*>} A promise that resolves when the operation has finished successfully
   */
  removePrefixed(database: string, store: string, prefix: string): Promise<any>;

  /**
   * Clears all stored values from a store.
   *
   * @param {!string} database The name of the database to use
   * @param {!string} store The name of the store to use
   * @return {!Promise<*>} A promise that resolves when the operation has finished successfully
   */
  clear(database: string, store: string): Promise<any>;

  /**
   * Provides a list with the keys for all the stored values, or those that start
   * with the given prefix.
   *
   * @param {!string} database The name of the database to use
   * @param {!string} store The name of the store to use
   * @param {string=} prefix The prefix to use
   * @return {!Promise<!Array.<string>>} A promise that resolves with the array of keys
   */
  keys(database: string, store: string, prefix?: string): Promise<string[]>;

  /**
   * Provides the number of key/value pairs in the store.
   *
   * @param {!string} database The name of the database to use
   * @param {!string} store The name of the store to use
   * @return {!Promise<number>} A promise that resolves with the number of elements stored
   */
  count(database: string, store: string): Promise<number>;

  /**
   * Close a store, if necessary.
   *
   * @param {!string} database The name of the database to use
   * @param {!string} store The name of the store to close
   * @return {!Promise<*>} A promise that resolves when the store has been successfuly closed
   */
  close(database: string, store: string): Promise<any>;

  /**
   * Create a new write batch for a given store
   *
   * @param {!string} database The name of the database to use
   * @param {!string} store The name of the store to use
   * @return {!StorageAdapterWriteBatch} The write batch object
   */
  writeBatch(database: string, store: string): StorageAdapterWriteBatch;

  /**
   * Returns a Promise that resolves with the estimated size in bytes of the store.
   *
   * @param {!string} database The name of the database to use
   * @param {!string} store The name of the store to use
   */
  estimatedSize(database: string, store: string): Promise<number>;
}

/**
 * An interface that represents a StorageAdapter write batch. One or more
 * write operations can be added to it before it's executed.
 *
 * The underlying storage adapter must guarantee that no operations will take
 * place until run() is called, and that the operations are run atomically: either
 * they all succeed or none of them do.
 */
export interface StorageAdapterWriteBatch {
  /**
   * Queues a set() operation on this write batch
   *
   * @param {!string} key The key name for the value to store
   * @param {?*=} value The value to store
   */
  set(key: string, value?: any);

  /**
   * Queues a remove() operation on this write batch
   *
   * @param {!string|Array.<!string>} keys The key name or array of key name for the values to remove
   */
  remove(keys: string | string[]);

  /**
   * Queues a removePrefixed() operation on this write batch
   *
   * @param {!string} prefix The prefix to use
   */
  removePrefixed(prefix: string);

  /**
   * Executes this write batch

   * @return {!Promise<*>} A promise that resolves when all the queued operations have finished
   */
  run(): Promise<any>;
}

/**
 * Checks if a provided StorageAdapter implementation has a valid API
 *
 * @param {string} apiName
 * @param {!StorageAdapter} storageAdapter The StorageAdapter implementation
 * @return {boolean}
 */
export const validateStorageAdapter = (
  apiName: string,
  storageAdapter: StorageAdapter
): boolean => {
  const isValid =
    typeof storageAdapter === 'object' &&
    ('get' in storageAdapter && typeof storageAdapter.get === 'function') &&
    ('getAll' in storageAdapter &&
      typeof storageAdapter.getAll === 'function') &&
    ('set' in storageAdapter && typeof storageAdapter.set === 'function') &&
    ('remove' in storageAdapter &&
      typeof storageAdapter.remove === 'function') &&
    ('removePrefixed' in storageAdapter &&
      typeof storageAdapter.removePrefixed === 'function') &&
    ('clear' in storageAdapter && typeof storageAdapter.clear === 'function') &&
    ('keys' in storageAdapter && typeof storageAdapter.keys === 'function') &&
    ('count' in storageAdapter && typeof storageAdapter.count === 'function') &&
    ('close' in storageAdapter && typeof storageAdapter.close === 'function') &&
    ('writeBatch' in storageAdapter &&
      typeof storageAdapter.writeBatch === 'function') &&
    ('estimatedSize' in storageAdapter &&
      typeof storageAdapter.estimatedSize === 'function') &&
    (!storageAdapter.writeThrottleTime ||
      typeof storageAdapter.writeThrottleTime === 'number') &&
    (!storageAdapter.maxServerCacheSize ||
      typeof storageAdapter.maxServerCacheSize === 'number');

  if (!isValid) {
    throw new Error(
      `${apiName} failed: The provided StorageAdapter implementation is not valid.`
    );
  }

  return isValid;
};

export interface KeyValueItem<T> {
  key: string;
  value: T;
}
