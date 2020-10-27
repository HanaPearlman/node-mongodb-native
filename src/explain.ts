import type { Server } from './sdam/server';
import { maxWireVersion } from './utils';

/** @public */
export enum ExplainVerbosity {
  queryPlanner = 'queryPlanner',
  queryPlannerExtended = 'queryPlannerExtended',
  executionStats = 'executionStats',
  allPlansExecution = 'allPlansExecution'
}

export const SUPPORTS_EXPLAIN_WITH_REMOVE = 3;
export const SUPPORTS_EXPLAIN_WITH_UPDATE = 3;
export const SUPPORTS_EXPLAIN_WITH_DISTINCT = 3.2;
export const SUPPORTS_EXPLAIN_WITH_FIND_AND_MODIFY = 3.2;
export const SUPPORTS_EXPLAIN_WITH_MAP_REDUCE = 4.4;

/** @public */
export interface ExplainOptions {
  /** Max secondary read staleness in seconds, Minimum value is 90 seconds.*/
  explain?: boolean | string;
}

/**
 * Checks that explain is supported by the server and operation.
 * @internal
 *
 * @param server - to check against
 * @param op - the operation to explain
 */
export function explainNotSupported(server: Server, op: string): boolean {
  const wireVersion = maxWireVersion(server);
  if (
    (op === 'remove' && wireVersion >= SUPPORTS_EXPLAIN_WITH_REMOVE) ||
    (op === 'update' && wireVersion >= SUPPORTS_EXPLAIN_WITH_UPDATE) ||
    (op === 'distinct' && wireVersion >= SUPPORTS_EXPLAIN_WITH_DISTINCT) ||
    (op === 'findAndModify' && wireVersion >= SUPPORTS_EXPLAIN_WITH_FIND_AND_MODIFY) ||
    (op === 'mapReduce' && wireVersion >= SUPPORTS_EXPLAIN_WITH_MAP_REDUCE)
  ) {
    return false;
  }

  return true;
}
