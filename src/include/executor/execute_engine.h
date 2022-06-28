#ifndef MINISQL_EXECUTE_ENGINE_H
#define MINISQL_EXECUTE_ENGINE_H

#include <string>
#include <unordered_map>
#include "common/dberr.h"
#include "common/instance.h"
#include "transaction/transaction.h"

extern "C" {
#include "parser/parser.h"
};

/**
 * ExecuteContext stores all the context necessary to run in the execute engine
 * This struct is implemented by student self for necessary.
 *
 * eg: transaction info, execute result...
 */
struct ExecuteContext {
  bool flag_quit_{false};
  bool condition_{true};
  int index_ind{-1};
  Transaction *txn_{nullptr};
};

/**
 * ExecuteEngine
 */
class ExecuteEngine {
public:
  ExecuteEngine();

  ~ExecuteEngine() {
    ExecuteContext __Context;
    ExecuteSaveDatabase(&__Context);
    for (auto it : dbs_) {
      delete it.second;
    }
  }

  /**
   * executor interface
   */
  dberr_t Execute(pSyntaxNode ast, ExecuteContext *context);

  // Database list save&load

  dberr_t ExecuteLoadDatabase(ExecuteContext *context);

  dberr_t ExecuteSaveDatabase(ExecuteContext *context);

private:
  dberr_t ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteSelect(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteInsert(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteDelete(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteUpdate(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context);

  dberr_t ExecuteQuit(pSyntaxNode ast, ExecuteContext *context);

  // Extra function: Condition judgement
  // Judge whether this data row satisfies the condition basing on the syntax tree
  dberr_t LogicConditions(pSyntaxNode ast, ExecuteContext *context, const Row &row, Schema* schema);

  // Extra function: Iterator selector
  // Find the first identifier with available index among conditions
  dberr_t SelectIterator(pSyntaxNode condition, ExecuteContext *context, const TableInfo *table, IndexInfo *&index,
                         RowId &begin_id, RowId &end_id, bool &covered);

private:
  [[maybe_unused]] std::unordered_map<std::string, DBStorageEngine *> dbs_;  /** all opened databases */
  [[maybe_unused]] std::string current_db_;  /** current database */
};

#endif //MINISQL_EXECUTE_ENGINE_H
