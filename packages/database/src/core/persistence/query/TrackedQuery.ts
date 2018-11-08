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

import { Query, QueryJSON } from '../../../api/Query';
import { Repo } from '../../Repo';

export interface TrackedQueryJSON {
  id: number;
  query: QueryJSON;
  lastUse: number;
  active: boolean;
  complete: boolean;
}

export class TrackedQuery {
  /**
   * Creates a new tracked query from its serializable form
   * @param repo
   * @param json
   * @return {any}
   */
  static fromJSON(json: TrackedQueryJSON, repo?: Repo): TrackedQuery | null {
    try {
      const query = Query.fromJSON(json['query'], repo);
      const trackedQuery = new TrackedQuery(
        json['id'],
        query,
        json['lastUse'],
        json['active']
      );
      trackedQuery.complete = json['complete'];
      return trackedQuery;
    } catch (err) {
      return null;
    }
  }

  static lastUseComparator(query1: TrackedQuery, query2: TrackedQuery) {
    if (query1.lastUse < query2.lastUse) {
      return -1;
    } else if (query1.lastUse > query2.lastUse) {
      return 1;
    } else {
      return 0;
    }
  }

  /*
  // for tests. Not used yet.
  static queryIdComparator(query1: TrackedQuery, query2: TrackedQuery) {
    if (query1.id < query2.id) {
      return -1;
    } else if (query1.id > query2.id) {
      return 1;
    } else {
      return 0;
    }
  }
  */

  constructor(
    public readonly id: number,
    public readonly query: Query,
    public lastUse: number,
    public active: boolean,
    public complete = false
  ) {}

  toJSON(): TrackedQueryJSON {
    return {
      id: this.id,
      query: this.query.toJSONObject(),
      lastUse: this.lastUse,
      active: this.active,
      complete: this.complete
    };
  }

  isEqual(other: TrackedQuery): boolean {
    if (!(other instanceof TrackedQuery)) {
      return false;
    }

    return (
      this.id === other.id &&
      this.query.isEqual(other.query) &&
      this.lastUse === other.lastUse &&
      this.active === other.active &&
      this.complete === other.complete
    );
  }
}
