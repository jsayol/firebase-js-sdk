/**
 * Copyright 2017 Google Inc.
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

import { assert } from '@firebase/util';
import { errorForServerCode } from './util/util';
import { AckUserWrite } from './operation/AckUserWrite';
import { ChildrenNode } from './snap/ChildrenNode';
import { contains, forEach, safeGet } from '@firebase/util';
import { ImmutableTree } from './util/ImmutableTree';
import { ListenComplete } from './operation/ListenComplete';
import { Merge } from './operation/Merge';
import { Operation, OperationSource } from './operation/Operation';
import { Overwrite } from './operation/Overwrite';
import { Path } from './util/Path';
import { SyncPoint } from './SyncPoint';
import { WriteTree, WriteTreeRef } from './WriteTree';
import { Query } from '../api/Query';
import { Node } from './snap/Node';
import { Event } from './view/Event';
import { EventRegistration } from './view/EventRegistration';
import { View } from './view/View';
import { PersistenceManager } from './persistence/PersistenceManager';
import { Repo } from './Repo';
import { resolveDeferredValueSnapshot } from './util/ServerValues';
import { CacheNode } from './view/CacheNode';

/**
 * @typedef {{
 *   startListening: function(
 *     !Query,
 *     ?number,
 *     function():string,
 *     function(!string, *):!Array.<!Event>
 *   ):!Array.<!Event>,
 *
 *   stopListening: function(!Query, ?number)
 * }}
 */
export interface ListenProvider {
  startListening(
    query: Query,
    tag: number | null,
    hashFn: () => string,
    onComplete: (a: string, b?: any) => Event[]
  ): Event[];

  stopListening(a: Query, b: number | null): void;
}

/**
 * SyncTree is the central class for managing event callback registration, data caching, views
 * (query processing), and event generation.  There are typically two SyncTree instances for
 * each Repo, one for the normal Firebase data, and one for the .info data.
 *
 * It has a number of responsibilities, including:
 *  - Tracking all user event callbacks (registered via addEventRegistration() and removeEventRegistration()).
 *  - Applying and caching data changes for user set(), transaction(), and update() calls
 *    (applyUserOverwrite(), applyUserMerge()).
 *  - Applying and caching data changes for server data changes (applyServerOverwrite(),
 *    applyServerMerge()).
 *  - Generating user-facing events for server and user changes (all of the apply* methods
 *    return the set of events that need to be raised as a result).
 *  - Maintaining the appropriate set of server listens to ensure we are always subscribed
 *    to the correct set of paths and queries to satisfy the current set of user event
 *    callbacks (listens are started/stopped using the provided listenProvider).
 *
 * NOTE: Although SyncTree tracks event callbacks and calculates events to raise, the actual
 * events are returned to the caller rather than raised synchronously.
 *
 * @constructor
 */
export class SyncTree {
  /**
   * Tree of SyncPoints.  There's a SyncPoint at any location that has 1 or more views.
   * @type {!ImmutableTree.<!SyncPoint>}
   * @private
   */
  private syncPointTree_: ImmutableTree<SyncPoint> = ImmutableTree.Empty;

  /**
   * A tree of all pending user writes (user-initiated set()'s, transaction()'s, update()'s, etc.).
   * @type {!WriteTree}
   * @private
   */
  private pendingWriteTree_ = new WriteTree();

  private tagToQueryMap_: { [k: string]: Query } = {};
  private queryToTagMap_: { [k: string]: number } = {};

  /**
   * Persistence manager
   */
  private persistenceManager_: PersistenceManager;

  /**
   * @param {!ListenProvider} listenProvider_ Used by SyncTree to start / stop listening
   *   to server data.
   */
  constructor(private listenProvider_: ListenProvider) {}

  /**
   * Set the persistence manager to use
   *
   * @param {!PersistenceManager} manager
   */
  set persistenceManager(manager: PersistenceManager) {
    this.persistenceManager_ = manager;
  }

  /**
   * Apply the data changes for a user-generated set() or transaction() call.
   *
   * @param {!Path} path
   * @param {!Node} newData
   * @param {number} writeId
   * @param {boolean=} visible
   * @return {!Array.<!Event>} Events to raise.
   */
  applyUserOverwrite(
    path: Path,
    newData: Node,
    writeId: number,
    visible?: boolean
  ): Event[] {
    // Record pending write.
    this.pendingWriteTree_.addOverwrite(path, newData, writeId, visible);

    if (!visible) {
      return [];
    } else {
      return this.applyOperationToSyncPoints_(
        new Overwrite(OperationSource.User, path, newData)
      );
    }
  }

  /**
   * Apply the data from a user-generated update() call
   *
   * @param {!Path} path
   * @param {!Object.<string, !Node>} changedChildren
   * @param {!number} writeId
   * @return {!Array.<!Event>} Events to raise.
   */
  applyUserMerge(
    path: Path,
    changedChildren: { [k: string]: Node },
    writeId: number
  ): Event[] {
    // Record pending merge.
    this.pendingWriteTree_.addMerge(path, changedChildren, writeId);

    const changeTree = ImmutableTree.fromObject(changedChildren);

    return this.applyOperationToSyncPoints_(
      new Merge(OperationSource.User, path, changeTree)
    );
  }

  /**
   * Acknowledge a pending user write that was previously registered with applyUserOverwrite() or applyUserMerge().
   *
   * @param {!number} writeId
   * @param {boolean=} revert True if the given write failed and needs to be reverted
   * @param {boolean=} persist True if the user write involves persistence
   * @param {Repo=} repo
   * @return {!Array.<!Event>} Events to raise.
   */
  ackUserWrite(
    writeId: number,
    revert = false,
    persist = false,
    repo?: Repo
  ): Event[] {
    const write = this.pendingWriteTree_.getWrite(writeId);
    const needToReevaluate = this.pendingWriteTree_.removeWrite(writeId);

    if (write.visible && persist && this.persistenceManager_ !== void 0) {
      this.persistenceManager_.removeUserWrite(writeId);

      if (!revert) {
        assert(repo, 'Cannot resolve deferred values without a repo instance');
        const serverValues = repo.generateServerValues();

        if (write.snap !== void 0) {
          const resolvedNode = resolveDeferredValueSnapshot(
            write.snap,
            serverValues
          );
          this.persistenceManager_.applyUserWrite(resolvedNode, write.path);
        } else {
          const resolvedMerge: { [k: string]: Node } = {};
          forEach(write.children, (childPath: string, childNode: Node) => {
            resolvedMerge[childPath] = resolveDeferredValueSnapshot(
              childNode,
              serverValues
            );
          });
          this.persistenceManager_.applyUserMerge(resolvedMerge, write.path);
        }
      }
    }

    if (!needToReevaluate) {
      return [];
    } else {
      let affectedTree = ImmutableTree.Empty;
      if (write.snap != null) {
        // overwrite
        affectedTree = affectedTree.set(Path.Empty, true);
      } else {
        forEach(write.children, function(pathString: string, node: Node) {
          affectedTree = affectedTree.set(new Path(pathString), node);
        });
      }
      return this.applyOperationToSyncPoints_(
        new AckUserWrite(write.path, affectedTree, revert)
      );
    }
  }

  /**
   * Apply new server data for the specified path..
   *
   * @param {!Path} path
   * @param {!Node} newData
   * @return {!Array.<!Event>} Events to raise.
   */
  applyServerOverwrite(path: Path, newData: Node): Event[] {
    if (this.persistenceManager_ !== void 0) {
      this.persistenceManager_.applyServerOverwrite(
        newData,
        Query.defaultAtPath(path)
      );
    }

    return this.applyOperationToSyncPoints_(
      new Overwrite(OperationSource.Server, path, newData)
    );
  }

  /**
   * Apply new server data to be merged in at the specified path.
   *
   * @param {!Path} path
   * @param {!Object.<string, !Node>} changedChildren
   * @return {!Array.<!Event>} Events to raise.
   */
  applyServerMerge(
    path: Path,
    changedChildren: { [k: string]: Node }
  ): Event[] {
    if (this.persistenceManager_ !== void 0) {
      this.persistenceManager_.applyServerMerge(changedChildren, path);
    }

    const changeTree = ImmutableTree.fromObject(changedChildren);

    return this.applyOperationToSyncPoints_(
      new Merge(OperationSource.Server, path, changeTree)
    );
  }

  /**
   * Apply a listen complete for a query
   *
   * @param {!Path} path
   * @return {!Array.<!Event>} Events to raise.
   */
  applyListenComplete(path: Path): Event[] {
    if (this.persistenceManager_ !== void 0) {
      this.persistenceManager_.setQueryComplete(Query.defaultAtPath(path));
    }

    return this.applyOperationToSyncPoints_(
      new ListenComplete(OperationSource.Server, path)
    );
  }

  /**
   * Apply new server data for the specified tagged query.
   *
   * @param {!Path} path
   * @param {!Node} snap
   * @param {!number} tag
   * @return {!Array.<!Event>} Events to raise.
   */
  applyTaggedQueryOverwrite(path: Path, snap: Node, tag: number): Event[] {
    const query = this.queryForTag_(tag);

    if (query != null) {
      const queryId = query.queryIdentifier();
      const relativePath = Path.relativePath(query.path, path);

      if (this.persistenceManager_ !== void 0) {
        const queryToOverwrite = relativePath.isEmpty()
          ? query
          : Query.defaultAtPath(path);
        this.persistenceManager_.applyServerOverwrite(snap, queryToOverwrite);
      }

      const op = new Overwrite(
        OperationSource.forServerTaggedQuery(queryId),
        relativePath,
        snap
      );
      return this.applyTaggedOperation_(query.path, op);
    } else {
      // Query must have been removed already
      return [];
    }
  }

  /**
   * Apply server data to be merged in for the specified tagged query.
   *
   * @param {!Path} path
   * @param {!Object.<string, !Node>} changedChildren
   * @param {!number} tag
   * @return {!Array.<!Event>} Events to raise.
   */
  applyTaggedQueryMerge(
    path: Path,
    changedChildren: { [k: string]: Node },
    tag: number
  ): Event[] {
    const query = this.queryForTag_(tag);

    if (query != null) {
      const queryId = query.queryIdentifier();
      const relativePath = Path.relativePath(query.path, path);
      const changeTree = ImmutableTree.fromObject(changedChildren);

      if (this.persistenceManager_ !== void 0) {
        this.persistenceManager_.applyServerMerge(changedChildren, path);
      }

      const op = new Merge(
        OperationSource.forServerTaggedQuery(queryId),
        relativePath,
        changeTree
      );

      return this.applyTaggedOperation_(query.path, op);
    } else {
      // We've already removed the query. No big deal, ignore the update
      return [];
    }
  }

  /**
   * Apply a listen complete for a tagged query
   *
   * @param {!Path} path
   * @param {!number} tag
   * @return {!Array.<!Event>} Events to raise.
   */
  applyTaggedListenComplete(path: Path, tag: number): Event[] {
    const query = this.queryForTag_(tag);
    if (query) {
      if (this.persistenceManager_ !== void 0) {
        this.persistenceManager_.setQueryComplete(query);
      }
      const relativePath = Path.relativePath(query.path, path);
      const op = new ListenComplete(
        OperationSource.forServerTaggedQuery(query.queryIdentifier()),
        relativePath
      );
      return this.applyTaggedOperation_(query.path, op);
    } else {
      // We've already removed the query. No big deal, ignore the update
      return [];
    }
  }

  /**
   * Add an event callback for the specified query.
   *
   * @param {!Query} query
   * @param {!EventRegistration} eventRegistration
   * @return {!Promise.<Array.<!Event>>} Promise that resolves to the events to raise.
   */
  addEventRegistration(
    query: Query,
    eventRegistration: EventRegistration
  ): Promise<Event[]> {
    const path = query.path;

    let serverCache: Node | null = null;
    let foundAncestorDefaultView = false;

    // Any covering writes will necessarily be at the root, so really all we need to find is the server cache.
    // Consider optimizing this once there's a better understanding of what actual behavior will be.
    this.syncPointTree_.foreachOnPath(path, function(
      pathToSyncPoint,
      syncPoint
    ) {
      const relativePath = Path.relativePath(pathToSyncPoint, path);
      serverCache =
        serverCache || syncPoint.getCompleteServerCache(relativePath);
      foundAncestorDefaultView =
        foundAncestorDefaultView || syncPoint.hasCompleteView();
    });

    let syncPoint = this.syncPointTree_.get(path);
    if (!syncPoint) {
      syncPoint = new SyncPoint(this.persistenceManager_);
      this.syncPointTree_ = this.syncPointTree_.set(path, syncPoint);
    } else {
      foundAncestorDefaultView =
        foundAncestorDefaultView || syncPoint.hasCompleteView();
      serverCache = serverCache || syncPoint.getCompleteServerCache(Path.Empty);
    }

    if (this.persistenceManager_ !== void 0) {
      this.persistenceManager_.setQueryActive(query);
    }

    const viewAlreadyExists = syncPoint.viewExistsForQuery(query);

    if (!viewAlreadyExists && !query.getQueryParams().loadsAllData()) {
      // We need to track a tag for this query
      const queryKey = SyncTree.makeQueryKey_(query);
      assert(
        !contains(this.queryToTagMap_, queryKey),
        'View does not exist, but we have a tag'
      );
      const tag = SyncTree.getNextQueryTag_();
      this.queryToTagMap_[queryKey] = tag;
      // Coerce to string to avoid sparse arrays.
      this.tagToQueryMap_['_' + tag] = query;
    }

    let eventsPromise: Promise<Event[]>;
    const writesCache = this.pendingWriteTree_.childWrites(path);

    if (serverCache !== null) {
      // We already found a complete server cache in memory
      const events = syncPoint.addEventRegistration(
        query,
        eventRegistration,
        writesCache,
        serverCache,
        true
      );
      eventsPromise = Promise.resolve(events);
    } else {
      if (this.persistenceManager_ !== void 0) {
        // Get the server cache from persistence
        eventsPromise = this.persistenceManager_
          .getServerCache(query)
          .then((cacheNode: CacheNode) => {
            return syncPoint.addEventRegistration(
              query,
              eventRegistration,
              writesCache,
              cacheNode.getNode(),
              cacheNode.isFullyInitialized()
            );
          });
      } else {
        // No complete server cache in memory, and persistence disabled
        serverCache = ChildrenNode.EMPTY_NODE as Node;

        const subtree = this.syncPointTree_.subtree(path);
        subtree.foreachChild((childName, childSyncPoint) => {
          const completeCache = childSyncPoint.getCompleteServerCache(
            Path.Empty
          );
          if (completeCache) {
            serverCache = serverCache.updateImmediateChild(
              childName,
              completeCache
            );
          }
        });

        const events = syncPoint.addEventRegistration(
          query,
          eventRegistration,
          writesCache,
          serverCache,
          false
        );
        eventsPromise = Promise.resolve(events);
      }
    }

    return eventsPromise.then((events: Event[]) => {
      if (!viewAlreadyExists && !foundAncestorDefaultView) {
        // There was no view and no default listen
        const view = syncPoint.viewForQuery(query);
        events = [...events, ...this.setupListener_(query, view)];
      }
      return events;
    });
  }

  /**
   * Remove event callback(s).
   *
   * If query is the default query, we'll check all queries for the specified eventRegistration.
   * If eventRegistration is null, we'll remove all callbacks for the specified query/queries.
   *
   * @param {!Query} query
   * @param {?EventRegistration} eventRegistration If null, all callbacks are removed.
   * @param {Error=} cancelError If a cancelError is provided, appropriate cancel events will be returned.
   * @return {!Array.<!Event>} Cancel events, if cancelError was provided.
   */
  removeEventRegistration(
    query: Query,
    eventRegistration: EventRegistration | null,
    cancelError?: Error
  ): Event[] {
    // Find the syncPoint first. Then deal with whether or not it has matching listeners
    const path = query.path;
    const maybeSyncPoint = this.syncPointTree_.get(path);
    let cancelEvents: Event[] = [];
    // A removal on a default query affects all queries at that location. A removal on an indexed query, even one without
    // other query constraints, does *not* affect all queries at that location. So this check must be for 'default', and
    // not loadsAllData().
    if (
      maybeSyncPoint &&
      (query.queryIdentifier() === Query.DefaultIdentifier ||
        maybeSyncPoint.viewExistsForQuery(query))
    ) {
      /**
       * @type {{removed: !Array.<!Query>, events: !Array.<!Event>}}
       */
      const removedAndEvents = maybeSyncPoint.removeEventRegistration(
        query,
        eventRegistration,
        cancelError
      );
      if (maybeSyncPoint.isEmpty()) {
        this.syncPointTree_ = this.syncPointTree_.remove(path);
      }
      const removed = removedAndEvents.removed;
      cancelEvents = removedAndEvents.events;
      // We may have just removed one of many listeners and can short-circuit this whole process
      // We may also not have removed a default listener, in which case all of the descendant listeners should already be
      // properly set up.
      //
      // Since indexed queries can shadow if they don't have other query constraints, check for loadsAllData(), instead of
      // queryId === 'default'
      const removingDefault =
        -1 !==
        removed.findIndex(function(query) {
          return query.getQueryParams().loadsAllData();
        });

      if (this.persistenceManager_ !== void 0) {
        removed.forEach((query: Query) =>
          this.persistenceManager_.setQueryInactive(query)
        );
      }

      const covered = this.syncPointTree_.findOnPath(path, function(
        relativePath,
        parentSyncPoint
      ) {
        return parentSyncPoint.hasCompleteView();
      });

      if (removingDefault && !covered) {
        const subtree = this.syncPointTree_.subtree(path);
        // There are potentially child listeners. Determine what if any listens we need to send before executing the
        // removal
        if (!subtree.isEmpty()) {
          // We need to fold over our subtree and collect the listeners to send
          const newViews = this.collectDistinctViewsForSubTree_(subtree);

          // Ok, we've collected all the listens we need. Set them up.
          for (let i = 0; i < newViews.length; ++i) {
            const view = newViews[i],
              newQuery = view.getQuery();
            const listener = this.createListenerForView_(view);
            this.listenProvider_.startListening(
              SyncTree.queryForListening_(newQuery),
              this.tagForQuery_(newQuery),
              listener.hashFn,
              listener.onComplete
            );
          }
        } else {
          // There's nothing below us, so nothing we need to start listening on
        }
      }
      // If we removed anything and we're not covered by a higher up listen, we need to stop listening on this query
      // The above block has us covered in terms of making sure we're set up on listens lower in the tree.
      // Also, note that if we have a cancelError, it's already been removed at the provider level.
      if (!covered && removed.length > 0 && !cancelError) {
        // If we removed a default, then we weren't listening on any of the other queries here. Just cancel the one
        // default. Otherwise, we need to iterate through and cancel each individual query
        if (removingDefault) {
          // We don't tag default listeners
          const defaultTag: number | null = null;
          this.listenProvider_.stopListening(
            SyncTree.queryForListening_(query),
            defaultTag
          );
        } else {
          removed.forEach((queryToRemove: Query) => {
            const tagToRemove = this.queryToTagMap_[
              SyncTree.makeQueryKey_(queryToRemove)
            ];
            this.listenProvider_.stopListening(
              SyncTree.queryForListening_(queryToRemove),
              tagToRemove
            );
          });
        }
      }
      // Now, clear all of the tags we're tracking for the removed listens
      this.removeTags_(removed);
    } else {
      // No-op, this listener must've been already removed
    }
    return cancelEvents;
  }

  /**
   * Returns a complete cache, if we have one, of the data at a particular path. The location must have a listener above
   * it, but as this is only used by transaction code, that should always be the case anyways.
   *
   * Note: this method will *include* hidden writes from transaction with applyLocally set to false.
   * @param {!Path} path The path to the data we want
   * @param {Array.<number>=} writeIdsToExclude A specific set to be excluded
   * @return {?Node}
   */
  calcCompleteEventCache(
    path: Path,
    writeIdsToExclude?: number[]
  ): Node | null {
    const includeHiddenSets = true;
    const writeTree = this.pendingWriteTree_;
    const serverCache = this.syncPointTree_.findOnPath(path, function(
      pathSoFar,
      syncPoint
    ) {
      const relativePath = Path.relativePath(pathSoFar, path);
      const serverCache = syncPoint.getCompleteServerCache(relativePath);
      if (serverCache) {
        return serverCache;
      }
    });
    return writeTree.calcCompleteEventCache(
      path,
      serverCache,
      writeIdsToExclude,
      includeHiddenSets
    );
  }

  /**
   * This collapses multiple unfiltered views into a single view, since we only need a single
   * listener for them.
   *
   * @param {!ImmutableTree.<!SyncPoint>} subtree
   * @return {!Array.<!View>}
   * @private
   */
  private collectDistinctViewsForSubTree_(
    subtree: ImmutableTree<SyncPoint>
  ): View[] {
    return subtree.fold<View[]>(
      (relativePath, maybeChildSyncPoint, childMap) => {
        if (maybeChildSyncPoint && maybeChildSyncPoint.hasCompleteView()) {
          const completeView = maybeChildSyncPoint.getCompleteView();
          return [completeView];
        } else {
          // No complete view here, flatten any deeper listens into an array
          let views: View[] = [];
          if (maybeChildSyncPoint) {
            views = maybeChildSyncPoint.getQueryViews();
          }
          forEach(childMap, function(key: string, childViews: View[]) {
            views = views.concat(childViews);
          });
          return views;
        }
      }
    );
  }

  /**
   * @param {!Array.<!Query>} queries
   * @private
   */
  private removeTags_(queries: Query[]) {
    for (let j = 0; j < queries.length; ++j) {
      const removedQuery = queries[j];
      if (!removedQuery.getQueryParams().loadsAllData()) {
        // We should have a tag for this
        const removedQueryKey = SyncTree.makeQueryKey_(removedQuery);
        const removedQueryTag = this.queryToTagMap_[removedQueryKey];
        delete this.queryToTagMap_[removedQueryKey];
        delete this.tagToQueryMap_['_' + removedQueryTag];
      }
    }
  }

  /**
   * Normalizes a query to a query we send the server for listening
   * @param {!Query} query
   * @return {!Query} The normalized query
   * @private
   */
  private static queryForListening_(query: Query): Query {
    if (
      query.getQueryParams().loadsAllData() &&
      !query.getQueryParams().isDefault()
    ) {
      // We treat queries that load all data as default queries
      // Cast is necessary because ref() technically returns Firebase which is actually fb.api.Firebase which inherits
      // from Query
      return /** @type {!Query} */ query.getRef();
    } else {
      return query;
    }
  }

  /**
   * For a given new listen, manage the de-duplication of outstanding subscriptions.
   *
   * @param {!Query} query
   * @param {!View} view
   * @return {!Array.<!Event>} This method can return events to support synchronous data sources
   * @private
   */
  private setupListener_(query: Query, view: View): Event[] {
    const path = query.path;
    const tag = this.tagForQuery_(query);
    const listener = this.createListenerForView_(view);

    const events = this.listenProvider_.startListening(
      SyncTree.queryForListening_(query),
      tag,
      listener.hashFn,
      listener.onComplete
    );

    const subtree = this.syncPointTree_.subtree(path);
    // The root of this subtree has our query. We're here because we definitely need to send a listen for that, but we
    // may need to shadow other listens as well.
    if (tag) {
      assert(
        !subtree.value.hasCompleteView(),
        "If we're adding a query, it shouldn't be shadowed"
      );
    } else {
      // Shadow everything at or below this location, this is a default listener.
      const queriesToStop = subtree.fold<Query[]>(function(
        relativePath,
        maybeChildSyncPoint,
        childMap
      ) {
        if (
          !relativePath.isEmpty() &&
          maybeChildSyncPoint &&
          maybeChildSyncPoint.hasCompleteView()
        ) {
          return [maybeChildSyncPoint.getCompleteView().getQuery()];
        } else {
          // No default listener here, flatten any deeper queries into an array
          let queries: Query[] = [];
          if (maybeChildSyncPoint) {
            queries = queries.concat(
              maybeChildSyncPoint.getQueryViews().map(view => view.getQuery())
            );
          }
          forEach(childMap, function(key: string, childQueries: Query[]) {
            queries = queries.concat(childQueries);
          });
          return queries;
        }
      });
      for (let i = 0; i < queriesToStop.length; ++i) {
        const queryToStop = queriesToStop[i];
        this.listenProvider_.stopListening(
          SyncTree.queryForListening_(queryToStop),
          this.tagForQuery_(queryToStop)
        );
      }
    }
    return events;
  }

  /**
   *
   * @param {!View} view
   * @return {{hashFn: function(), onComplete: function(!string, *)}}
   * @private
   */
  private createListenerForView_(
    view: View
  ): { hashFn(): string; onComplete(a: string, b?: any): Event[] } {
    const query = view.getQuery();
    const tag = this.tagForQuery_(query);

    return {
      hashFn: () => {
        const cache = view.getServerCache() || ChildrenNode.EMPTY_NODE;
        return cache.hash();
      },
      onComplete: (status: string): Event[] => {
        if (status === 'ok') {
          if (tag) {
            return this.applyTaggedListenComplete(query.path, tag);
          } else {
            return this.applyListenComplete(query.path);
          }
        } else {
          // If a listen failed, kill all of the listeners here, not just the one that triggered the error.
          // Note that this may need to be scoped to just this listener if we change permissions on filtered children
          const error = errorForServerCode(status, query);
          return this.removeEventRegistration(
            query,
            /*eventRegistration*/ null,
            error
          );
        }
      }
    };
  }

  /**
   * Given a query, computes a "queryKey" suitable for use in our queryToTagMap_.
   * @private
   * @param {!Query} query
   * @return {string}
   */
  private static makeQueryKey_(query: Query): string {
    return query.path.toString() + '$' + query.queryIdentifier();
  }

  /**
   * Return the query associated with the given tag, if we have one
   * @param {!number} tag
   * @return {?Query}
   * @private
   */
  private queryForTag_(tag: number): Query | null {
    return this.tagToQueryMap_['_' + tag] || null;
  }

  /**
   * Return the tag associated with the given query.
   * @param {!Query} query
   * @return {?number}
   * @private
   */
  private tagForQuery_(query: Query): number | null {
    const queryKey = SyncTree.makeQueryKey_(query);
    return safeGet(this.queryToTagMap_, queryKey);
  }

  /**
   * Static tracker for next query tag.
   * @type {number}
   * @private
   */
  private static nextQueryTag_ = 1;

  /**
   * Static accessor for query tags.
   * @return {number}
   * @private
   */
  private static getNextQueryTag_(): number {
    return SyncTree.nextQueryTag_++;
  }

  /**
   * A helper method to apply tagged operations
   *
   * @param {!Path} queryPath
   * @param {!Operation} operation
   * @return {!Array.<!Event>}
   * @private
   */
  private applyTaggedOperation_(
    queryPath: Path,
    operation: Operation
  ): Event[] {
    const syncPoint = this.syncPointTree_.get(queryPath);
    assert(syncPoint, "Missing sync point for query tag that we're tracking");
    const writesCache = this.pendingWriteTree_.childWrites(queryPath);
    return syncPoint.applyOperation(
      operation,
      writesCache,
      /*serverCache=*/ null
    );
  }

  /**
   * A helper method that visits all descendant and ancestor SyncPoints, applying the operation.
   *
   * NOTES:
   * - Descendant SyncPoints will be visited first (since we raise events depth-first).

   * - We call applyOperation() on each SyncPoint passing three things:
   *   1. A version of the Operation that has been made relative to the SyncPoint location.
   *   2. A WriteTreeRef of any writes we have cached at the SyncPoint location.
   *   3. A snapshot Node with cached server data, if we have it.

   * - We concatenate all of the events returned by each SyncPoint and return the result.
   *
   * @param {!Operation} operation
   * @return {!Array.<!Event>}
   * @private
   */
  private applyOperationToSyncPoints_(operation: Operation): Event[] {
    return this.applyOperationHelper_(
      operation,
      this.syncPointTree_,
      /*serverCache=*/ null,
      this.pendingWriteTree_.childWrites(Path.Empty)
    );
  }

  /**
   * Recursive helper for applyOperationToSyncPoints_
   *
   * @private
   * @param {!Operation} operation
   * @param {ImmutableTree.<!SyncPoint>} syncPointTree
   * @param {?Node} serverCache
   * @param {!WriteTreeRef} writesCache
   * @return {!Array.<!Event>}
   */
  private applyOperationHelper_(
    operation: Operation,
    syncPointTree: ImmutableTree<SyncPoint>,
    serverCache: Node | null,
    writesCache: WriteTreeRef
  ): Event[] {
    if (operation.path.isEmpty()) {
      return this.applyOperationDescendantsHelper_(
        operation,
        syncPointTree,
        serverCache,
        writesCache
      );
    } else {
      const syncPoint = syncPointTree.get(Path.Empty);

      // If we don't have cached server data, see if we can get it from this SyncPoint.
      if (serverCache == null && syncPoint != null) {
        serverCache = syncPoint.getCompleteServerCache(Path.Empty);
      }

      let events: Event[] = [];
      const childName = operation.path.getFront();
      const childOperation = operation.operationForChild(childName);
      const childTree = syncPointTree.children.get(childName);
      if (childTree && childOperation) {
        const childServerCache = serverCache
          ? serverCache.getImmediateChild(childName)
          : null;
        const childWritesCache = writesCache.child(childName);
        events = events.concat(
          this.applyOperationHelper_(
            childOperation,
            childTree,
            childServerCache,
            childWritesCache
          )
        );
      }

      if (syncPoint) {
        events = events.concat(
          syncPoint.applyOperation(operation, writesCache, serverCache)
        );
      }

      return events;
    }
  }

  /**
   * Recursive helper for applyOperationToSyncPoints_
   *
   * @private
   * @param {!Operation} operation
   * @param {ImmutableTree.<!SyncPoint>} syncPointTree
   * @param {?Node} serverCache
   * @param {!WriteTreeRef} writesCache
   * @return {!Array.<!Event>}
   */
  private applyOperationDescendantsHelper_(
    operation: Operation,
    syncPointTree: ImmutableTree<SyncPoint>,
    serverCache: Node | null,
    writesCache: WriteTreeRef
  ): Event[] {
    const syncPoint = syncPointTree.get(Path.Empty);

    // If we don't have cached server data, see if we can get it from this SyncPoint.
    if (serverCache == null && syncPoint != null) {
      serverCache = syncPoint.getCompleteServerCache(Path.Empty);
    }

    let events: Event[] = [];
    syncPointTree.children.inorderTraversal((childName, childTree) => {
      const childServerCache = serverCache
        ? serverCache.getImmediateChild(childName)
        : null;
      const childWritesCache = writesCache.child(childName);
      const childOperation = operation.operationForChild(childName);
      if (childOperation) {
        events = events.concat(
          this.applyOperationDescendantsHelper_(
            childOperation,
            childTree,
            childServerCache,
            childWritesCache
          )
        );
      }
    });

    if (syncPoint) {
      events = events.concat(
        syncPoint.applyOperation(operation, writesCache, serverCache)
      );
    }

    return events;
  }
}
