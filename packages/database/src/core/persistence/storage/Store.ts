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

import {
  KeyValueItem,
  StorageAdapter,
  StorageAdapterWriteBatch
} from './StorageAdapter';
import { fatal } from '../../util/util';

/**
 * Store name for data coming from the server (locations with attached listeners)
 *
 * @type {string}
 * @const
 */
export const SERVER_STORE_NAME = 'server';

/**
 * Store name for pending write operations from the user
 *
 * @type {string}
 * @const
 */
export const USER_STORE_NAME = 'user';

/**
 * Store name for tracked queries
 *
 * @type {string}
 * @const
 */
export const QUERIES_STORE_NAME = 'query';

/**
 * List of stores that are available. Any store name not listed here is not valid.
 *
 * @type {[string,string]}
 */
export const VALID_STORES = [
  SERVER_STORE_NAME,
  USER_STORE_NAME,
  QUERIES_STORE_NAME
];

/**
 * Represents a store used for data persistence.
 * Basically just a wrapper around a StorageAdapter for a specific store.
 *
 * TODO(jsayol): implement write throttling
 */
export class Store {
  /**
   * @param {!string} database_ The name of the database for this store
   * @param {!string} store_ The name of this store
   * @param {!StorageAdapter} storageAdapter_ The storage adapter to use
   */
  constructor(
    private database_: string,
    private store_: string,
    private storageAdapter_: StorageAdapter
  ) {
    validatePersistenceStoreName(this.store_);
  }

  get(key: string): Promise<any> {
    return this.storageAdapter_.get(this.database_, this.store_, key);
  }

  getAll<T>(prefix?: string): Promise<KeyValueItem<T>[]> {
    return this.storageAdapter_.getAll(this.database_, this.store_, prefix);
  }

  set(key: string, value?: any): Promise<any> {
    return this.storageAdapter_.set(this.database_, this.store_, key, value);
  }

  remove(key: string): Promise<any> {
    return this.storageAdapter_.remove(this.database_, this.store_, key);
  }

  removePrefixed(prefix: string): Promise<any> {
    return this.storageAdapter_.removePrefixed(
      this.database_,
      this.store_,
      prefix
    );
  }

  clear(): Promise<any> {
    return this.storageAdapter_.clear(this.database_, this.store_);
  }

  keys(prefix?: string): Promise<string[]> {
    return this.storageAdapter_.keys(this.database_, this.store_, prefix);
  }

  count(): Promise<number> {
    return this.storageAdapter_.count(this.database_, this.store_);
  }

  close(): Promise<any> {
    return this.storageAdapter_.close(this.database_, this.store_);
  }

  writeBatch(): StorageAdapterWriteBatch {
    return this.storageAdapter_.writeBatch(this.database_, this.store_);
  }

  estimatedSize(): Promise<number> {
    return this.storageAdapter_.estimatedSize(this.database_, this.store_);
  }
}

const validatePersistenceStoreName = (name: string) => {
  if (VALID_STORES.indexOf(name) < 0) {
    fatal(`"${name}" is not a valid persistence store name.`);
  }
};
