import type { Document } from '.';
import type { Server } from './sdam/server';
import { maxWireVersion } from './utils';

export enum ExplainVerbosity {
  queryPlanner = 'queryPlanner',
  queryPlannerExtended = 'queryPlannerExtended',
  executionStats = 'executionStats',
  allPlansExecution = 'allPlansExecution'
}

// export enum ExplainableOpString {
//   remove = 'remove',
//   update = 'update',
//   distinct = 'distinct',
//   findAndModify = 'findAndModify',
//   mapReduce = 'mapReduce'
// }

export const SUPPORTS_EXPLAIN_WITH_REMOVE = 3;
export const SUPPORTS_EXPLAIN_WITH_UPDATE = 3;
export const SUPPORTS_EXPLAIN_WITH_DISTINCT = 3.2;
export const SUPPORTS_EXPLAIN_WITH_FIND_AND_MODIFY = 3.2;
export const SUPPORTS_EXPLAIN_WITH_MAP_REDUCE = 4.4;

export type Verbosity =
  | boolean
  | ExplainVerbosity.queryPlannerExtended
  | ExplainVerbosity.queryPlannerExtended
  | ExplainVerbosity.executionStats
  | ExplainVerbosity.allPlansExecution;

/** @public */
export interface ExplainOptions {
  /**
   * Represents the requested verbosity of the explain. Valid values include a boolean
   * or any of the ExplainVerbosity strings.
   * */
  explain?: Verbosity;
}

/**
 * Checks that explain is supported by the server and operation.
 * @internal
 *
 * @param server - to check against
 * @param op - the operation to explain
 */
export function explainSupported(server: Server, op: string): boolean {
  const wireVersion = maxWireVersion(server);
  if (
    (op === 'remove' && wireVersion >= SUPPORTS_EXPLAIN_WITH_REMOVE) ||
    (op === 'update' && wireVersion >= SUPPORTS_EXPLAIN_WITH_UPDATE) ||
    (op === 'distinct' && wireVersion >= SUPPORTS_EXPLAIN_WITH_DISTINCT) ||
    (op === 'findAndModify' && wireVersion >= SUPPORTS_EXPLAIN_WITH_FIND_AND_MODIFY) ||
    (op === 'mapReduce' && wireVersion >= SUPPORTS_EXPLAIN_WITH_MAP_REDUCE)
  ) {
    return true;
  }

  return false;
}
// technically doesn't need to be used but what if using driver w non-typescript?
export function validExplainVerbosity(verbosity: boolean | string): boolean {
  if (typeof verbosity === 'string') {
    return (
      verbosity === ExplainVerbosity.queryPlanner ||
      verbosity === ExplainVerbosity.queryPlannerExtended ||
      verbosity === ExplainVerbosity.allPlansExecution ||
      verbosity === ExplainVerbosity.executionStats
    );
  }
  return true;
}
