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

import { Path } from '../../util/Path';
import { ImmutableTree } from '../../util/ImmutableTree';
import { assertionError } from '@firebase/util';

const PRUNE_TREE = new ImmutableTree<boolean>(true);
const KEEP_TREE = new ImmutableTree<boolean>(false);

const PRUNE_PREDICATE = (value: boolean) => value;
const KEEP_PREDICATE = (value: boolean) => !value;

export class PruneForest {
  static readonly Empty = new PruneForest(ImmutableTree.Empty);

  constructor(private pruneForest_: ImmutableTree<boolean>) {}

  prunesAnything(): boolean {
    return this.pruneForest_.containsValueMatching(PRUNE_PREDICATE);
  }

  shouldPruneUnkeptDescendants(path: Path): boolean {
    const shouldPrune = this.pruneForest_.findLeafMostValue(path);
    return shouldPrune !== null && shouldPrune;
  }

  prunePath(path: Path): PruneForest {
    if (
      this.pruneForest_.findRootMostMatchingPathAndValue(path, KEEP_PREDICATE)
    ) {
      throw assertionError("Can't prune path that was kept previously");
    }

    if (
      this.pruneForest_.findRootMostMatchingPathAndValue(path, KEEP_PREDICATE)
    ) {
      // This path will already be pruned
      return this;
    }

    return new PruneForest(this.pruneForest_.setTree(path, PRUNE_TREE));
  }

  keepPath(path: Path): PruneForest {
    if (
      this.pruneForest_.findRootMostMatchingPathAndValue(path, KEEP_PREDICATE)
    ) {
      // This path will already be kept
      return this;
    }

    return new PruneForest(this.pruneForest_.setTree(path, KEEP_TREE));
  }

  // The following methods are only needed for the tests, which aren't implemented yet
  /*
  shouldKeepPath(path: Path): boolean {
    const shouldPrune = this.pruneForest_.findLeafMostValue(path);
    return (shouldPrune !== null) && !shouldPrune;
  }

  affectsPath(path: Path): boolean {
    return (this.pruneForest_.findRootMostValueAndPath(path) !== null) ||
      !this.pruneForest_.subtree(path).isEmpty();
  }


  child(childKey: string): PruneForest {
    let childPruneForest = this.pruneForest_.children.get(childKey);

    if (childPruneForest === null) {
      if (this.pruneForest_.value !== null) {
        childPruneForest = this.pruneForest_.value ? PRUNE_TREE : KEEP_TREE;
      } else {
        childPruneForest = ImmutableTree.Empty;
      }
    } else {
      if ((childPruneForest === null) && (this.pruneForest_.value !== null)) {
        childPruneForest = childPruneForest.set(Path.Empty, this.pruneForest_.value);
      }
    }

    return new PruneForest(childPruneForest);
  }

  childAtPath(path: Path): PruneForest {
    if (path.isEmpty()) {
      return this;
    } else {
      return this.child(path.getFront()).childAtPath(path.popFront());
    }
  }

  forEachKeptNode(fn: (path: Path) => void) {
    this.pruneForest_.foreach((path: Path, value: boolean) => {
      if (value) {
        fn(path);
      }
    });
  }

  keepAll(path: Path, children: string[]): PruneForest {
    if (this.pruneForest_.findRootMostMatchingPathAndValue(path, KEEP_PREDICATE)) {
      // This path will already be kept
      return this;
    }

    return this.setPruneValue_(path, children, KEEP_TREE);
  }

  pruneAll(path: Path, children: string[]): PruneForest {
    if (this.pruneForest_.findRootMostMatchingPathAndValue(path, KEEP_PREDICATE)) {
      throw assertionError('Can\'t prune path that was kept previously');
    }

    if (this.pruneForest_.findRootMostMatchingPathAndValue(path, PRUNE_PREDICATE)) {
      // This path will already be pruned
      return this;
    }

    return this.setPruneValue_(path, children, PRUNE_TREE);
  }

  private setPruneValue_(path: Path, children: string[], value: ImmutableTree<boolean>): PruneForest {
    const subtree = this.pruneForest_.subtree(path);
    let childrenMap = subtree.children;

    children.forEach((childKey: string) => {
      childrenMap = childrenMap.insert(childKey, value);
    });

    const newSubtree = new ImmutableTree(subtree.value, childrenMap);
    return new PruneForest(this.pruneForest_.setTree(path, newSubtree));
  }
  */
}
