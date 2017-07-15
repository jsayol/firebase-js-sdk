import { assert } from './utils/assert';
import { forEach } from './utils/obj';
import { Repo } from './database/core/Repo';
import { warn } from './database/core/util/util';
import { Path } from './database/core/util/Path';
import { resolveDeferredValueSnapshot } from './database/core/util/ServerValues';
import { nodeFromJSON } from './database/core/snap/nodeFromJSON';
import { Node } from './database/core/snap/Node';
import { StorageAdapter } from './database/persistence/storage/StorageAdapter';
import { IDBStorageAdapter } from './database/persistence/storage/IDBStorageAdapter';
import { PersistedUserWrite } from './database/persistence/UserWriteStore';
import { PersistenceManager } from './database/persistence/PersistenceManager';
import { LRUCachePolicy } from './database/persistence/cache/CachePolicy';

declare module './database/core/Repo' {
  interface Repo {
    enablePersistence(storageAdapter?: StorageAdapter | null): void;

    closePersistence(): void;

    restoreWrites_(): void;
  }
}

Repo.prototype.enablePersistence = function (storageAdapter?: StorageAdapter | null) {
  if (this.persistenceManager_ !== void 0) {
    warn('Database persistence was already enabled and cannot be changed.');
    return;
  }

  try {
    if (!storageAdapter) {
      storageAdapter = new IDBStorageAdapter();
    }

    const maxCacheSize = storageAdapter.maxServerCacheSize || void 0;
    const cachePolicy = new LRUCachePolicy(maxCacheSize);
    this.persistenceManager_ = new PersistenceManager(this, cachePolicy, storageAdapter);
  } catch (error) {
    // Something went wrong when initializing persistence. It's possible that the platform
    // where we're running doesn't support the storage adapter we're trying to use.
    this.persistenceManager_ = void 0;
    warn('Failed to initialize database persistence. It will be disabled.', error);

    // TODO(jsayol): maybe we should offer a way for the user to detect the error programatically,
    // in case they want to retry enabling persistence with a different storage adapter.
    return;
  }

  this.log_('Persistence enabled');
  this.serverSyncTree_.persistenceManager = this.persistenceManager_;
  this.restoreWrites_();
};

Repo.prototype.closePersistence = function () {
  if (this.persistenceManager_ !== void 0) {
    this.persistenceManager_.close();
  }
};

Repo.prototype.restoreWrites_ = function () {
  this.persistenceManager_.getUserWrites()
    .then((writes: PersistedUserWrite[]) => {
      const serverValues = this.generateServerValues();
      let lastWriteId = Number.NEGATIVE_INFINITY;

      const callback = (write: PersistedUserWrite, op: string, status: string, errorReason: string) => {
        const success = status === 'ok';
        if (!success) {
          warn(`Persisted ${op} at ${write.path} failed: ${status} - ${errorReason}`);
        }

        const clearEvents = this.serverSyncTree_.ackUserWrite(write.id, !success, true, this);
        this.eventQueue_.raiseEventsForChangedPath(new Path(write.path), clearEvents);
      };

      writes.forEach((write: PersistedUserWrite) => {
        assert(lastWriteId < write.id, 'Restored writes were not in order');
        lastWriteId = write.id;
        this.nextWriteId_ = write.id + 1;

        const path = new Path(write.path);

        if (write.overwrite) {
          this.log_(`Restoring overwrite with id ${write.id} at path ${write.path}`);
          const unresolvedNode = nodeFromJSON(write.overwrite);
          const resolvedNode = resolveDeferredValueSnapshot(unresolvedNode, serverValues);
          this.serverSyncTree_.applyUserOverwrite(path, resolvedNode, write.id, true);
          this.server_.put(write.path, write.overwrite, callback.bind(this, write, 'set'));
        } else {
          this.log_(`Restoring merge with id ${write.id} at path ${write.path}`);
          const resolvedMerge: { [k: string]: Node } = {};
          forEach(write.merge, (childPath: string, childNode: Node) => {
            const unresolvedNode = nodeFromJSON(childNode);
            resolvedMerge[childPath] = resolveDeferredValueSnapshot(unresolvedNode, serverValues);
          });

          this.serverSyncTree_.applyUserMerge(path, resolvedMerge, write.id);
          this.server_.merge(write.path, write.merge, callback.bind(this, write, 'update'));
        }
      });
    })
    .catch((error: Error) => {
      warn('Failed to restore persisted user writes', error);
    });
};
