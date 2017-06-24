import { Query, QueryJSON } from '../../api/Query';
import { Repo } from '../../core/Repo';

export interface TrackedQueryJSON {
  id: number;
  query: QueryJSON;
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
      const query = Query.fromJSON(json.query, repo);
      const trackedQuery = new TrackedQuery(json.id, query, json.active);
      trackedQuery.complete_ = json.complete;
      return trackedQuery;
    } catch (err) {
      return null;
    }
  }

  static queryIdComparator(query1: TrackedQuery, query2: TrackedQuery) {
    if (query1.id < query2.id) {
      return -1;
    } else if (query1.id > query2.id) {
      return 1;
    } else {
      return 0;
    }
  }

  constructor(public readonly id: number,
              public readonly query: Query,
              private active_: boolean,
              private complete_ = false) {
  }

  toJSON(): TrackedQueryJSON {
    return {
      id: this.id,
      query: this.query.toJSONObject(),
      active: this.active_,
      complete: this.complete_
    };
  }

  isEqual(other: TrackedQuery): boolean {
    if (!(other instanceof TrackedQuery)) {
      return false;
    }

    return (this.id === other.id)
      && (this.query.isEqual(other.query))
      && (this.active_ === other.active_)
      && (this.complete_ === other.complete_);
  }

  get active() {
    return this.active_;
  }

  set active(value: boolean) {
    this.active_ = value;
  }

  get complete() {
    return this.complete_;
  }

  set complete(value: boolean) {
    this.complete_ = value;
  }

}