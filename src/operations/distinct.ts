import { Aspect, defineAspects } from './operation';
import { CommandOperation, CommandOperationOptions } from './command';
import { decorateWithCollation, decorateWithReadConcern, Callback, maxWireVersion } from '../utils';
import type { Document } from '../bson';
import type { Server } from '../sdam/server';
import type { Collection } from '../collection';
import { MongoError } from '../error';
import { ExplainOptions, SUPPORTS_EXPLAIN_WITH_DISTINCT, validExplainVerbosity } from '../explain';

/** @public */
export interface DistinctOptions extends CommandOperationOptions, ExplainOptions {}

/** @internal Return a list of distinct values for the given key across a collection. */
export class DistinctOperation extends CommandOperation<DistinctOptions, Document[]> {
  collection: Collection;
  /** Field of the document to find distinct values for. */
  key: string;
  /** The query for filtering the set of documents to which we apply the distinct filter. */
  query: Document;

  /**
   * Construct a Distinct operation.
   *
   * @param collection - Collection instance.
   * @param key - Field of the document to find distinct values for.
   * @param query - The query for filtering the set of documents to which we apply the distinct filter.
   * @param options - Optional settings. See Collection.prototype.distinct for a list of options.
   */
  constructor(collection: Collection, key: string, query: Document, options?: DistinctOptions) {
    super(collection, options);

    this.collection = collection;
    this.key = key;
    this.query = query;
  }

  execute(server: Server, callback: Callback<Document[]>): void {
    const coll = this.collection;
    const key = this.key;
    const query = this.query;
    const options = this.options;

    // Distinct command
    const cmd: Document = {
      distinct: coll.collectionName,
      key: key,
      query: query
    };

    // Add maxTimeMS if defined
    if (typeof options.maxTimeMS === 'number') {
      cmd.maxTimeMS = options.maxTimeMS;
    }

    // Do we have a readConcern specified
    decorateWithReadConcern(cmd, coll, options);

    // Have we specified collation
    try {
      decorateWithCollation(cmd, coll, options);
    } catch (err) {
      return callback(err);
    }

    if (options.explain) {
      if (!validExplainVerbosity(options.explain)) {
        callback(new MongoError(`${options.explain} is an invalid explain verbosity`));
        return;
      }

      if (maxWireVersion(server) < SUPPORTS_EXPLAIN_WITH_DISTINCT) {
        callback(
          new MongoError('the current topology does not support explain on distinct commands')
        );
        return;
      }

      cmd.explain = options.explain;
    }

    super.executeCommand(server, cmd, (err, result) => {
      if (err) {
        callback(err);
        return;
      }

      callback(undefined, this.options.fullResponse || options.explain ? result : result.values);
    });
  }
}

defineAspects(DistinctOperation, [Aspect.READ_OPERATION, Aspect.RETRYABLE]);
