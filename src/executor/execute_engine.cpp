#include <cstdio>
#include <chrono>
#include <fstream>
#include "executor/execute_engine.h"
#include "glog/logging.h"

// Width of each column in selected table
constexpr uint32_t DISPLAY_COLUMN_WIDTH = 12;

extern "C" {
int yyparse(void);
//FILE *yyin;
#include "parser/minisql_lex.h"
#include "parser/parser.h"
}

// ============ < FUNCTION > ============

// These functions are used in executor methods.

/* Convert string (char array form) to MiniSQL TypeId.
   It's designed for syntax tree node (ColumnType) analysis.
*/
TypeId GetTypeId(const char *str) {
  switch (!strcmp(str, "int") + 2 * !strcmp(str, "float") + 4 * !strcmp(str, "char")) {
    case 1:
      return kTypeInt;
    case 2:
      return kTypeFloat;
    case 4:
      return kTypeChar;
    default:
      break;
  }
  return kTypeInvalid;
}

/* Convert dberr_t to string (error type).
*/
std::string GetErrorIdentifier(dberr_t error) {
  static const std::string __Ids[] = {  "DB_SUCCESS",
                                        "DB_FAILED",
                                        "DB_TABLE_ALREADY_EXIST",
                                        "DB_TABLE_NOT_EXIST",
                                        "DB_INDEX_ALREADY_EXIST",
                                        "DB_INDEX_NOT_FOUND",
                                        "DB_COLUMN_NAME_NOT_EXIST",
                                        "DB_KEY_NOT_FOUND"};
  return __Ids[error].c_str();
}

/* Check if a numeric string is float
*/
bool isFloat(char* cp) {
  // Check float
  bool isFloat = false;
  for (char *c = cp; *c != '\0'; ++c)
    if (*c == '.') {
      isFloat = true;
      break;
    }
  return isFloat;
}

/* Complement str to length max_len with prefix space.
   If the input c_str has a length greater than max_len, replace the
    last two characters with '.'
 */
inline char *CStringComplement(const char *str, uint32_t max_len) {
  static char ret[256];
  uint32_t len = 0;
  while (str[len] != '\0' && len <= max_len) ++len;
  if (len > max_len) {
    ret[--len] = '\0', ret[0] = str[0];
    ret[--len] = '.', ret[--len] = '.';
    while (--len > 0) ret[len] = str[len];
  } else {
    ret[max_len] = '\0';
    if (len == max_len) {
      while (--len > 0) ret[len] = str[len];
      ret[0] = str[0];
    } else if (len == 0) {
      ret[0] = ' ';
      while (++len < max_len) ret[len] = ' ';
    } else {
      ret[max_len] = '\0', len = max_len - len;
      while (--max_len - len > 0) ret[max_len] = str[max_len - len];
      ret[len] = str[0], ret[0] = ' ';
      while (--len > 0) ret[len] = ' ';
    }
  }
  return ret;
}

/* The author thinks this function (you are reading now)
   is not perfect to deal with all cases.
*/
void __Unperfect__Function() {
  printf("[[WARNING]] This function haven't been well implemented yet so you may get some fried rice from it.");
}

/* Unfold syntax tree on output.
*/
void __Unflod__SyntaxTree(pSyntaxNode ast, int depth) {
  static const std::string __TypeName[] = {
    "kNodeUnknown", "kNodeQuit", "kNodeExecFile", "kNodeIdentifier", "kNodeNumber",
    "kNodeString", "kNodeNull", "kNodeCreateDB", "kNodeDropDB", "kNodeShowDB",
    "kNodeUseDB", "kNodeShowTables", "kNodeCreateTable", "kNodeDropTable", "kNodeShowIndexes",
    "kNodeInsert", "kNodeDelete", "kNodeUpdate", "kNodeSelect", "kNodeConditions",
    "kNodeConnector", "kNodeCompareOperator", "kNodeColumnType", "kNodeColumnDefinition", "kNodeColumnDefinitionList",
    "kNodeColumnList", "kNodeColumnValues", "kNodeUpdateValues", "kNodeUpdateValue", "kNodeAllColumns",
    "kNodeCreateIndex", "kNodeDropIndex", "kNodeIndexType", "kNodeTrxBegin", "kNodeTrxCommit",
    "kNodeTrxRollback"
  };
  for (int i = 0; i < depth; ++i) printf("\t");
  printf("%s(%s)",__TypeName[ast->type_].c_str(),ast->val_);
  if(ast->child_) {
    printf(" {\n");
    __Unflod__SyntaxTree(ast->child_, depth + 1);
    printf("\n");
    for (int i = 0; i < depth; ++i) printf("\t");
    printf("}");
  }
  if(ast->next_) {
    printf("\n");
    __Unflod__SyntaxTree(ast->next_, depth);
  }
}

// ============ < EXECUTOR > ============

ExecuteEngine::ExecuteEngine() { 
  current_db_ = "";
  // Load databases
  ExecuteContext __Context;
  ExecuteLoadDatabase(&__Context);
}

// Database list save&load

dberr_t ExecuteEngine::ExecuteLoadDatabase(ExecuteContext *context) {
  // Load database list from file
  char DatabaseName[64];
  ifstream ListFile("Minisql.dblist");

  if (ListFile.is_open()) {
    while (!ListFile.eof()) {
      ListFile.getline(DatabaseName, 63);
      std::string DatabaseNameString = std::string(DatabaseName);
      if (DatabaseNameString.length() < 1) continue;
      dbs_.insert(
        std::pair<std::string, DBStorageEngine *>(
          DatabaseNameString, 
          new DBStorageEngine(DatabaseNameString, false)
      ));
    }
    if (dbs_.size() < 1)
      printf("MiniSQL: No databases available now.\n");
    else
      printf("Successfully load %lu database(s) from disk.\n", dbs_.size());
  } else {
    printf("MiniSQL: No databases available now.\n");
    return DB_FAILED;
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteSaveDatabase(ExecuteContext *context) {
  // Save database name list to file
  ofstream ListFile("Minisql.dblist");

  if (ListFile.is_open()) {
    for (auto pa : dbs_) {
      ListFile << pa.first;
    }
    ListFile.close();
  } else {
    printf("Failed to write database list to disk.\n");
    return DB_FAILED;
  }

  return DB_SUCCESS;
}

dberr_t ExecuteEngine::Execute(pSyntaxNode ast, ExecuteContext *context) {
  if (ast == nullptr) {
    return DB_FAILED;
  }
  switch (ast->type_) {
    case kNodeCreateDB:
      return ExecuteCreateDatabase(ast, context);
    case kNodeDropDB:
      return ExecuteDropDatabase(ast, context);
    case kNodeShowDB:
      return ExecuteShowDatabases(ast, context);
    case kNodeUseDB:
      return ExecuteUseDatabase(ast, context);
    case kNodeShowTables:
      return ExecuteShowTables(ast, context);
    case kNodeCreateTable:
      return ExecuteCreateTable(ast, context);
    case kNodeDropTable:
      return ExecuteDropTable(ast, context);
    case kNodeShowIndexes:
      return ExecuteShowIndexes(ast, context);
    case kNodeCreateIndex:
      return ExecuteCreateIndex(ast, context);
    case kNodeDropIndex:
      return ExecuteDropIndex(ast, context);
    case kNodeSelect:
      return ExecuteSelect(ast, context);
    case kNodeInsert:
      return ExecuteInsert(ast, context);
    case kNodeDelete:
      return ExecuteDelete(ast, context);
    case kNodeUpdate:
      return ExecuteUpdate(ast, context);
    case kNodeTrxBegin:
      return ExecuteTrxBegin(ast, context);
    case kNodeTrxCommit:
      return ExecuteTrxCommit(ast, context);
    case kNodeTrxRollback:
      return ExecuteTrxRollback(ast, context);
    case kNodeExecFile:
      return ExecuteExecfile(ast, context);
    case kNodeQuit:
      return ExecuteQuit(ast, context);
    default:
      break;
  }
  return DB_FAILED;
}


dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "[ExecuteCreateDatabase]" << std::endl;
#endif
  auto astIden = ast->child_;
  if (astIden->type_ != kNodeIdentifier) {
    printf("Invalid identifier.\n");
    return DB_FAILED;
  }

  auto astIdenStr = std::string(astIden->val_);     // Identifier
  auto DBSEptr = new DBStorageEngine(astIdenStr);
  if (DBSEptr == nullptr) {
    printf("Failed when allocate memory for new database.\n");
    return DB_FAILED;
  }

  dbs_.insert(std::pair<std::string, DBStorageEngine *>(astIdenStr, DBSEptr));
  current_db_ = astIdenStr;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "[ExecuteDropDatabase]" << std::endl;
#endif
  auto astIden = ast->child_;
  if (astIden->type_ != kNodeIdentifier) {
    printf("Invalid identifier.\n");
    return DB_FAILED;
  }

  auto astIdenStr = std::string(astIden->val_);  // Identifier
  auto dbs_iter = dbs_.find(astIdenStr);
  if (dbs_iter != dbs_.end()) {
    if (current_db_ == astIdenStr) 
      current_db_ = "";
    delete dbs_iter->second;    // Drop database
    dbs_.erase(dbs_iter);
  } else {
    printf("Database not found.\n");
    return DB_FAILED;
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "[ExecuteShowDatabases]" << std::endl;
#endif
  for (auto p : dbs_) {
    printf(" %s\n", p.first.c_str());
  }
  printf("%ld database(s) available now.\n", dbs_.size());
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "[ExecuteUseDatabase]" << std::endl;
#endif
  auto astIden = ast->child_;
  if (astIden->type_ != kNodeIdentifier) {
    printf("Invalid identifier.\n");
    return DB_FAILED;
  }

  // Check database
  std::string DatabaseName = std::string(astIden->val_);
  if (dbs_.find(DatabaseName) != dbs_.end()) {
    current_db_ = DatabaseName;
  } else {
    printf("Database %s not found.\n", DatabaseName.c_str());
    return DB_FAILED;
  }
  
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "[ExecuteShowTables]" << std::endl;
#endif
  if (!strcmp(current_db_.c_str(), "")) {
    printf("Never specified any database yet.\n");
    return DB_FAILED;
  }

  std::vector<TableInfo *> TableList;   // Table list of current DB
  dberr_t GetReturn = 
    dbs_[current_db_]->catalog_mgr_->
    GetTables(TableList);               // Get table list from catalog manager
  if (GetReturn == DB_SUCCESS) {
    // Show all table name
    for (auto T : TableList) {
      printf(" %s\n", T->GetTableName().c_str());
    }
    printf("%ld table(s) in database %s.\n", TableList.size(), current_db_.c_str());
  } else {
    printf("Failed to get table(s) in database %s.\n", current_db_.c_str());
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "[ExecuteCreateTable]" << std::endl;
#endif
  if (!strcmp(current_db_.c_str(), "")) {
    printf("Never specified any database yet.\n");
    return DB_FAILED;
  }

  auto astIden = ast->child_;
  if (astIden->type_ != kNodeIdentifier) {
    printf("Invalid identifier.\n");
    return DB_FAILED;
  }
  TableInfo *NewTableInfo;                               // Create TableInfo
  auto NewTableIdentifier = std::string(astIden->val_);  // Identifier
  if (dbs_[current_db_]->catalog_mgr_->GetTable(NewTableIdentifier, NewTableInfo) == DB_SUCCESS) {
    printf("There is already a table which named %s!\n", NewTableIdentifier.c_str());
    return DB_FAILED;
  }

  // Get column definition list
  if (astIden->next_ && astIden->next_->type_ == kNodeColumnDefinitionList &&
      astIden->next_->child_) {

    bool __Unique;
    uint32_t __Index = 0;
    auto astCol = astIden->next_->child_;
    std::vector<Column *> ColumnList;
    std::vector<std::string> UniqueKeyList;
    std::vector<std::string> PrimaryKeyList;

    // Get parameters
    while (astCol) {

      if (astCol->type_ == kNodeColumnDefinition) {
        // Crisis check (TO DO)
        // bool IdentifierCollided = false;
        
        std::string NewColumnName = std::string(astCol->child_->val_);
        TypeId NewColumnType = GetTypeId(astCol->child_->next_->val_);
        __Unique = (astCol->val_ && strcmp(astCol->val_, "unique") == 0) ? true : false;
        if (__Unique) UniqueKeyList.push_back(NewColumnName);

        // Insertion
        if (NewColumnType == kTypeChar) {
          // CHAR
          auto ColumnLength = astCol->child_->next_->child_->val_;
          if (isFloat(ColumnLength) || atoi(ColumnLength) <= 0) {
            printf("Unexpected char length on column %lu.\n", ColumnList.size());
            return DB_FAILED;
          }
          ColumnList.push_back(new Column(
            NewColumnName,                      // Name
            NewColumnType,                      // Type
            atoi(ColumnLength),                 // Length
            __Index++,                          // Index, start from 0
            true,                               // Nullable
            __Unique                            // Unique
          ));
        } else {
          // NON-CHAR
          ColumnList.push_back(new Column(
            NewColumnName,                      // Name
            NewColumnType,                      // Type
            __Index++,                          // Index, start from 0
            true,                               // Nullable
            __Unique                            // Unique
          ));
        }
      
      } else if (astCol->type_ == kNodeColumnList) {
        
        // PRIMARY KEYS
        auto astPrimaryIden = astCol->child_;
        while (astPrimaryIden) {

          PrimaryKeyList.push_back(string(astPrimaryIden->val_));

          // Find key by name
          uint32_t KeyIndex = 0;
          for (; KeyIndex < ColumnList.size(); ++KeyIndex)
            if (strcmp(astPrimaryIden->val_, ColumnList[KeyIndex]->GetName().c_str()) == 0)
              break;
          
          // Set this column to not null & unique
          if (KeyIndex != ColumnList.size()) {
            Column* C = ColumnList[KeyIndex];
            ColumnList[KeyIndex] = new Column (
              C->GetName(), C->GetType(), C->GetTableInd(), false, true
            );
          }
          astPrimaryIden = astPrimaryIden->next_;   // Next identifier
        }
      }

      astCol = astCol->next_;   // Next node
    }

    TableSchema *NewTableSchema = new Schema(ColumnList);       // Create Schema
    dberr_t CreateReturn =
      dbs_[current_db_]->catalog_mgr_->
      CreateTable (
        NewTableIdentifier,     // table_name
        NewTableSchema,         // schema
        nullptr,                // txn
        NewTableInfo            // table_info
      );
    if (CreateReturn != DB_SUCCESS) {
      printf("Failed to create table %s in database %s.\n", NewTableIdentifier.c_str(), current_db_.c_str());
      return DB_FAILED;
    }
    
    // Create Indexes for primary keys and unique keys
    // Every unique column has a index respectly while all primary keys share one index.
    IndexInfo *__IndexInfo;
    std::vector<std::string> UniqueIndexColumn;
    for (auto __Col : UniqueKeyList) {
      UniqueIndexColumn.clear();
      UniqueIndexColumn.push_back(__Col);
      CreateReturn = dbs_[current_db_]->catalog_mgr_->CreateIndex(NewTableIdentifier, "_" + __Col + "_Index",
                                                                  UniqueIndexColumn, nullptr, __IndexInfo);
      if (CreateReturn != DB_SUCCESS) {
        printf("Failed to create index for unique key %s.\n", __Col.c_str());
      }
    }
    std::vector<std::string> PrimaryIndexColumn;
    for (auto __Col : PrimaryKeyList)
      PrimaryIndexColumn.push_back(__Col);
    if (PrimaryIndexColumn.size() > 0) {
      CreateReturn = dbs_[current_db_]->catalog_mgr_->CreateIndex(
          NewTableIdentifier, "_" + NewTableIdentifier + "_Index_Primary", PrimaryIndexColumn, nullptr, __IndexInfo);
      if (CreateReturn != DB_SUCCESS) {
        printf("Failed to create index for primary keys.\n");
      }
    }

  }
  else {
    // Syntax tree isn't organized in correct structure.
    printf("Syntax analysis error.\n");
    return DB_FAILED;
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "[ExecuteDropTable]" << std::endl;
#endif
  if (!strcmp(current_db_.c_str(), "")) {
    printf("Never specified any database yet.\n");
    return DB_FAILED;
  }
  
  // Get parameter identifier
  auto astIden = ast->child_;
  if (astIden->type_ != kNodeIdentifier) {
    printf("Invalid identifier.\n");
    return DB_FAILED;
  }
  auto astIdenStr = std::string(astIden->val_);
  
  dberr_t DropReturn =
    dbs_[current_db_]->catalog_mgr_->
    DropTable(astIdenStr);

  if (DropReturn == DB_SUCCESS) {
    return DB_SUCCESS;
  } else if (DropReturn == DB_TABLE_NOT_EXIST) {
    printf("Table %s not found.\n", astIdenStr.c_str());
    return DB_FAILED;
  } else {
    printf("Failed to drop table %s in database %s.\n", astIdenStr.c_str(), current_db_.c_str());
    return DB_FAILED;
  }
}

dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "[ExecuteShowIndexes]" << std::endl;
#endif
  if (!strcmp(current_db_.c_str(), "")) {
    printf("Never specified any database yet.\n");
    return DB_FAILED;
  }

  std::vector<TableInfo *> TableList;                       // Table list of current DB
  dberr_t GetReturn = dbs_[current_db_]->catalog_mgr_->
    GetTables(TableList);                                   // Get table list from catalog manager

  if (GetReturn == DB_SUCCESS) {

    // List all indexes ..
    int IndexCount = 0;
    std::vector<IndexInfo *> TableIndexes;
    for (auto Tp : TableList) {
      
      // For each table in current DB, get index list
      TableIndexes.clear();
      dberr_t GetIndexesReturn = dbs_[current_db_]->catalog_mgr_->
        GetTableIndexes(Tp->GetTableName(), TableIndexes);
      
      if (GetIndexesReturn == DB_SUCCESS) {
        // OUTPUT FORMAT:
        // index_name on table_name(attributes)
        for (auto Ip : TableIndexes) {
          
          // Hide indices create by CREATE TABLE
          if (Ip->GetIndexName().length() > 0 && Ip->GetIndexName().at(0) == '_') continue;

          printf("  %s on %s(", Ip->GetIndexName().c_str(), Tp->GetTableName().c_str());
          ++IndexCount;
          if (Ip->GetIndexKeySchema()->GetColumns().size() > 0) {
            // For each column in index Ip:
            printf("%s", Ip->GetIndexKeySchema()->GetColumns()[0]->GetName().c_str());
            for (uint32_t i = 1; i < Ip->GetIndexKeySchema()->GetColumns().size(); ++i) {
              printf(", %s", Ip->GetIndexKeySchema()->GetColumns()[i]->GetName().c_str());
            }
            // I'm nearly dizzy #$^@(%SD^(V)@%@^+:+@!%_^
          }
          printf(")\n");
        }
        
      } else {
        printf("Failed when search indexes from table %s.\n", Tp->GetTableName().c_str());
      }
    }

    // Print number of index
    printf("%d Indices in database %s.\n", IndexCount, current_db_.c_str());

  } else {
    printf("Failed to get table(s) in database %s.\n", current_db_.c_str());
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "[ExecuteCreateIndex]" << std::endl;
#endif
  if (!strcmp(current_db_.c_str(), "")) {
    printf("Never specified any database yet.\n");
    return DB_FAILED;
  }

  // Para 1: Index identifier
  auto astIden = ast->child_;
  if (astIden->type_ != kNodeIdentifier) {
    printf("Invalid index identifier.\n");
    return DB_FAILED;
  }
  auto NewIndexName = std::string(astIden->val_);

  // Para 2: Table identifier
  astIden = astIden->next_;
  if (astIden->type_ != kNodeIdentifier) {
    printf("Invalid table identifier.\n");
    return DB_FAILED;
  }
  auto TableName = std::string(astIden->val_);
  TableInfo* __Ti;
  if (dbs_[current_db_]->catalog_mgr_->GetTable(TableName, __Ti) != DB_SUCCESS) {
    printf("Table %s does not exist!\n", TableName.c_str());
    return DB_FAILED;
  }

  // Para 3: Column list
  astIden = astIden->next_;
  auto astCol = astIden->child_;    // Column Identifiers
  std::vector<std::string> NewIndexColumns;
  while (astCol) {
    NewIndexColumns.push_back(std::string(astCol->val_));
    astCol = astCol->next_;     // Next identifier
  }
  dberr_t GetReturn;
  uint32_t __Idx;
  std::vector<uint32_t> KeyMap;
  for (auto s : NewIndexColumns) {
    GetReturn = __Ti->GetSchema()->GetColumnIndex(s, __Idx);
    if (GetReturn != DB_SUCCESS) {
      printf("Column %s does not exist!\n", s.c_str());
      return DB_FAILED;
    }
    KeyMap.push_back(__Idx);
  }

  // Para 4: Index type (optional)
  //astIden = ast->next_;
  //if (astIden) Change index type;
  
  IndexInfo *NewIndexInfo;
  if (dbs_[current_db_]->catalog_mgr_->GetIndex(TableName, NewIndexName, NewIndexInfo) == DB_SUCCESS) {
    printf("There is already an index which named %s on table %s!\n", NewIndexName.c_str(), TableName.c_str());
    return DB_FAILED;
  }
  NewIndexInfo = nullptr;
  dberr_t CreateReturn = 
    dbs_[current_db_]->catalog_mgr_->
      CreateIndex(TableName, NewIndexName, NewIndexColumns, nullptr, NewIndexInfo);
  if (CreateReturn != DB_SUCCESS || NewIndexInfo == nullptr) {
    printf("Failed to create index on table %s.\n", TableName.c_str());
    return DB_FAILED;
  }

  // Full table insert!
  dberr_t InsertEntryReturn;
  std::vector<Field> IndexFields;
  auto CurrentIterator = __Ti->GetTableHeap()->Begin(nullptr);
  auto TableEnd = __Ti->GetTableHeap()->End();

  uint32_t SelectedRow = 0;
  for (; CurrentIterator != TableEnd; ++CurrentIterator) {

    // Select this row:
    IndexFields.clear();
    for (auto i : KeyMap)
      IndexFields.push_back(Field(*(CurrentIterator->GetField(i))));
    InsertEntryReturn = NewIndexInfo->GetIndex()->InsertEntry(Row(IndexFields), CurrentIterator->GetRowId(), nullptr);

    if (InsertEntryReturn != DB_SUCCESS) {
      printf("Construct index error. There may be duplicate value in index column(s).\n");
      dberr_t DropReturn = dbs_[current_db_]->catalog_mgr_->DropIndex(TableName, NewIndexName);
      if (DropReturn != DB_SUCCESS) {
        printf("Failed to rollback creation of index %s.\n", NewIndexName.c_str());
      }
      return DB_FAILED;
    }
    ++SelectedRow;
  }
  printf("Successfully create %s with %u row(s).\n", NewIndexName.c_str(), SelectedRow);

  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "[ExecuteDropIndex]" << std::endl;
#endif
  if (!strcmp(current_db_.c_str(), "")) {
    printf("Never specified any database yet.\n");
    return DB_FAILED;
  }

  // Index name
  auto astIden = ast->child_;
  if (astIden->type_ != kNodeIdentifier) {
    printf("Invalid index identifier.\n");
    return DB_FAILED;
  }
  auto IndexName = std::string(astIden->val_);

  std::vector<TableInfo *> TableList;                       // Table list of current DB
  dberr_t GetReturn = dbs_[current_db_]->catalog_mgr_->
    GetTables(TableList);                                   // Get table list from catalog manager

  bool IndexFound = false;
  if (GetReturn == DB_SUCCESS) {

    dberr_t DropReturn;
    IndexInfo *TargetIndexInfo;

    for (auto Tp : TableList) {
      if (DB_SUCCESS == 
          dbs_[current_db_]->catalog_mgr_->GetIndex(Tp->GetTableName(), IndexName, TargetIndexInfo)) {
        // Got it!
        DropReturn = dbs_[current_db_]->catalog_mgr_->DropIndex(Tp->GetTableName(), IndexName);
        if (DropReturn != DB_SUCCESS) {
          printf("Failed to drop index %s.\n", IndexName.c_str());
          return DB_FAILED;
        }
        IndexFound = true;
      }
      if (IndexFound) break;
    }
    
  }

  if (!IndexFound) {
    printf("Index %s not found.\n", IndexName.c_str());
    return DB_FAILED;
  }

  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteSelect(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "[ExecuteSelect]" << std::endl;
#endif
  if (!strcmp(current_db_.c_str(), "")) {
    printf("Never specified any database yet.\n");
    return DB_FAILED;
  }

  std::vector<std::string> SelectColumnNames;
  if (ast->child_->type_ == kNodeColumnList) {
    auto astCol = ast->child_->child_;
    while (astCol) {
      SelectColumnNames.push_back(std::string(astCol->val_));
      astCol = astCol->next_;   // Next column
    }
  } else if (ast->child_->type_ == kNodeAllColumns) {
    SelectColumnNames.clear();  // size 0 => All columns
  } else {
    printf("Unexpected column parameters!\n");
    return DB_FAILED;
  }
  
  TableInfo *__Ti;
  dberr_t GetTableReturn =
    dbs_[current_db_]->catalog_mgr_->GetTable(ast->child_->next_->val_, __Ti);
  if (GetTableReturn != DB_SUCCESS) {
    printf("Table %s not found!\n", ast->child_->next_->val_);
    return DB_FAILED;
  }
  auto CurrentIterator = __Ti->GetTableHeap()->Begin(nullptr);

  // Generate index array
  dberr_t FindColumnReturn;
  uint32_t __Idx;   // Temp
  std::vector<uint32_t> SelectIndexes;
  // Find ..
  if (SelectColumnNames.size() > 0) {
    // Select columns in vector SelectColumnNames
    for (auto ColumnStr : SelectColumnNames) {
      FindColumnReturn = __Ti->GetSchema()->GetColumnIndex(ColumnStr, __Idx);
      if (FindColumnReturn != DB_SUCCESS) {
        printf("Column %s not found!\n", ColumnStr.c_str());
        return DB_FAILED;
      }
      SelectIndexes.push_back(__Idx);
    }
  } else {
    // Select all columns
    std::vector<Column *> __Col = __Ti->GetSchema()->GetColumns();
    for (uint32_t i = 0; i < __Col.size(); ++i) {
      SelectColumnNames.push_back(__Col[i]->GetName());
      SelectIndexes.push_back(i);
    }
    printf("\n");
  }

  auto ConditionRoot = ast->child_->next_->next_;

  // ================ < SELECT > ================
  uint32_t SelectedRow = 0;
  dberr_t LogicReturn;
  ExecuteContext IteratorContext;

  IndexInfo *__IndexInfo = nullptr;
  RowId BeginID, EndID;
  bool BoundaryCovered;
  dberr_t IteratorReturn =
      SelectIterator(ConditionRoot, &IteratorContext, __Ti, __IndexInfo, BeginID, EndID, BoundaryCovered);
  if (IteratorReturn != DB_SUCCESS) {
    printf("Index loading error!\n");
    return DB_FAILED;
  }

  // Print title
  printf("+");
  for (uint32_t i = 0; i < SelectColumnNames.size() * (DISPLAY_COLUMN_WIDTH + 3) - 1; ++i) printf("-");
  printf("+\n|");
  for (auto __Str : SelectColumnNames) printf(" %s |", CStringComplement(__Str.c_str(), DISPLAY_COLUMN_WIDTH));
  printf("\n+");
  for (uint32_t i = 0; i < SelectColumnNames.size() * (DISPLAY_COLUMN_WIDTH + 3) - 1; ++i) printf("-");
  printf("+\n");

  // ============= < SELECT BEGIN > =============
  auto SelectBegin = std::chrono::steady_clock::now();

  if (IteratorContext.index_ind >= 0) {

    // Process boundary
    // Table Row => Fields (of Index Schema) => Index Row => Iterator

    if (!__IndexInfo) {
      printf("Iterator selector chooses a null index pointer!\n");
      return DB_FAILED;
    }

    bool ID2RowReturn;
    Row BeginRow = Row(BeginID), EndRow = Row(EndID);
    std::vector<uint32_t> KeyMap;
    std::vector<Field> BeginFields, EndFields;
    for (auto __Col : __IndexInfo->GetIndexKeySchema()->GetColumns()) {
      FindColumnReturn = __Ti->GetSchema()->GetColumnIndex(__Col->GetName(), __Idx);
      if (FindColumnReturn != DB_SUCCESS) {
        printf("Column %s in index %s not found!\n", __Col->GetName().c_str(), __IndexInfo->GetIndexName().c_str());
        return DB_FAILED;
      }
      KeyMap.push_back(__Idx);
    }

    if (!(BeginID == INVALID_ROWID)) {  // What? RowId does not have operator!= ?!
      ID2RowReturn = __Ti->GetTableHeap()->GetTuple(&BeginRow, nullptr);
      if (!ID2RowReturn) {
        printf("Index %s provides wrong RowID when determining search range begining.\n", __IndexInfo->GetIndexName().c_str());
        return DB_FAILED;
      }
      for (auto __Key : KeyMap) BeginFields.push_back(Field(*(BeginRow.GetField(__Key))));
    }
    if (!(EndID == INVALID_ROWID)) {
      ID2RowReturn = __Ti->GetTableHeap()->GetTuple(&EndRow, nullptr);
      if (!ID2RowReturn) {
        printf("Index %s provides wrong RowID when determining search range end.\n", __IndexInfo->GetIndexName().c_str());
        return DB_FAILED;
      }
      for (auto __Key : KeyMap) EndFields.push_back(Field(*(EndRow.GetField(__Key))));
    }
    
    // Iterate by Index

    uint32_t KeySize = (4U << __IndexInfo->BpTreeType());
    switch (KeySize) {
      // PROCESS OF INDEX INTERATE:
      //  1. Get Iterator
      //  2. For each loop:
      //       Condition judge
      //       Display this row
      case 4: {

        IndexInfo::INDEX_KEY_TYPE4 BeginIndexKey, EndIndexKey;
        if (!(BeginID == INVALID_ROWID)) {
          Row BeginIndexRow = Row(BeginFields);
          BeginIndexKey.SerializeFromKey(BeginIndexRow, __IndexInfo->GetIndexKeySchema());
        }
        if (!(EndID == INVALID_ROWID)) {
          Row EndIndexRow = Row(EndFields);
          EndIndexKey.SerializeFromKey(EndIndexRow, __IndexInfo->GetIndexKeySchema());
        }
        auto CurrentIndexIterator = (BeginID == INVALID_ROWID)
                                        ? __IndexInfo->GetBTreeIndex4()->GetBeginIterator()
                                        : __IndexInfo->GetBTreeIndex4()->GetBeginIterator(BeginIndexKey);
        if (!BoundaryCovered && !(BeginID == INVALID_ROWID)) CurrentIndexIterator++;
        auto IndexEnd = (EndID == INVALID_ROWID) ? __IndexInfo->GetBTreeIndex4()->GetEndIterator()
                                                 : __IndexInfo->GetBTreeIndex4()->GetBeginIterator(EndIndexKey);
        if (BoundaryCovered && !(EndID == INVALID_ROWID)) IndexEnd++;

        // Main Loop
        bool GetReturn;
        for (; CurrentIndexIterator != IndexEnd; ++CurrentIndexIterator) {
          ExecuteContext SelectContext;
          Row thisRow = Row((*CurrentIndexIterator).second);
          GetReturn =  __Ti->GetTableHeap()->GetTuple(&thisRow, nullptr);
          if (!GetReturn) {
            printf("Index %s provides wrong RowID when fetching data.\n", __IndexInfo->GetIndexName().c_str());
            return DB_FAILED;
          }
          LogicReturn = LogicConditions(ConditionRoot, &SelectContext, thisRow, __Ti->GetSchema());
          if (LogicReturn != DB_SUCCESS) {
            printf("Failed to analyze logic conditions.\n");
            return DB_FAILED;
          }

          if (SelectContext.condition_) {
            // Select this row:
            ++SelectedRow;
            printf("|");
            // Print columns
            for (auto i : SelectIndexes) {
              printf(" %s |",
                     CStringComplement(
                         (thisRow.GetField(i)->IsNull()) ? "(null)" : thisRow.GetField(i)->GetData(),
                         DISPLAY_COLUMN_WIDTH));
            }
            printf("\n");
          }
        }
      }
        break;
      case 8: {

        IndexInfo::INDEX_KEY_TYPE8 BeginIndexKey, EndIndexKey;
        if (!(BeginID == INVALID_ROWID)) {
          Row BeginIndexRow = Row(BeginFields);
          BeginIndexKey.SerializeFromKey(BeginIndexRow, __IndexInfo->GetIndexKeySchema());
        }
        if (!(EndID == INVALID_ROWID)) {
          Row EndIndexRow = Row(EndFields);
          EndIndexKey.SerializeFromKey(EndIndexRow, __IndexInfo->GetIndexKeySchema());
        }
        auto CurrentIndexIterator = (BeginID == INVALID_ROWID)
                                        ? __IndexInfo->GetBTreeIndex8()->GetBeginIterator()
                                        : __IndexInfo->GetBTreeIndex8()->GetBeginIterator(BeginIndexKey);
        if (!BoundaryCovered && !(BeginID == INVALID_ROWID)) CurrentIndexIterator++;
        auto IndexEnd = (EndID == INVALID_ROWID) ? __IndexInfo->GetBTreeIndex8()->GetEndIterator()
                                                 : __IndexInfo->GetBTreeIndex8()->GetBeginIterator(EndIndexKey);
        if (BoundaryCovered && !(EndID == INVALID_ROWID)) IndexEnd++;

        // Main Loop
        bool GetReturn;
        for (; CurrentIndexIterator != IndexEnd; ++CurrentIndexIterator) {
          ExecuteContext SelectContext;
          Row thisRow = Row((*CurrentIndexIterator).second);
          GetReturn = __Ti->GetTableHeap()->GetTuple(&thisRow, nullptr);
          if (!GetReturn) {
            printf("Index %s provides wrong RowID when fetching data.\n", __IndexInfo->GetIndexName().c_str());
            return DB_FAILED;
          }
          LogicReturn = LogicConditions(ConditionRoot, &SelectContext, thisRow, __Ti->GetSchema());
          if (LogicReturn != DB_SUCCESS) {
            printf("Failed to analyze logic conditions.\n");
            return DB_FAILED;
          }

          if (SelectContext.condition_) {
            // Select this row:
            ++SelectedRow;
            printf("|");
            // Print columns
            for (auto i : SelectIndexes) {
              printf(" %s |",
                     CStringComplement((thisRow.GetField(i)->IsNull()) ? "(null)" : thisRow.GetField(i)->GetData(),
                                       DISPLAY_COLUMN_WIDTH));
            }
            printf("\n");
          }
        }
      }
        break;
      case 16: {

        IndexInfo::INDEX_KEY_TYPE16 BeginIndexKey, EndIndexKey;
        if (!(BeginID == INVALID_ROWID)) {
          Row BeginIndexRow = Row(BeginFields);
          BeginIndexKey.SerializeFromKey(BeginIndexRow, __IndexInfo->GetIndexKeySchema());
        }
        if (!(EndID == INVALID_ROWID)) {
          Row EndIndexRow = Row(EndFields);
          EndIndexKey.SerializeFromKey(EndIndexRow, __IndexInfo->GetIndexKeySchema());
        }
        auto CurrentIndexIterator = (BeginID == INVALID_ROWID)
                                        ? __IndexInfo->GetBTreeIndex16()->GetBeginIterator()
                                        : __IndexInfo->GetBTreeIndex16()->GetBeginIterator(BeginIndexKey);
        if (!BoundaryCovered && !(BeginID == INVALID_ROWID)) CurrentIndexIterator++;
        auto IndexEnd = (EndID == INVALID_ROWID) ? __IndexInfo->GetBTreeIndex16()->GetEndIterator()
                                                 : __IndexInfo->GetBTreeIndex16()->GetBeginIterator(EndIndexKey);
        if (BoundaryCovered && !(EndID == INVALID_ROWID)) IndexEnd++;

        // Main Loop
        bool GetReturn;
        for (; CurrentIndexIterator != IndexEnd; ++CurrentIndexIterator) {
          ExecuteContext SelectContext;
          Row thisRow = Row((*CurrentIndexIterator).second);
          GetReturn = __Ti->GetTableHeap()->GetTuple(&thisRow, nullptr);
          if (!GetReturn) {
            printf("Index %s provides wrong RowID when fetching data.\n", __IndexInfo->GetIndexName().c_str());
            return DB_FAILED;
          }
          LogicReturn = LogicConditions(ConditionRoot, &SelectContext, thisRow, __Ti->GetSchema());
          if (LogicReturn != DB_SUCCESS) {
            printf("Failed to analyze logic conditions.\n");
            return DB_FAILED;
          }

          if (SelectContext.condition_) {
            // Select this row:
            ++SelectedRow;
            printf("|");
            // Print columns
            for (auto i : SelectIndexes) {
              printf(" %s |",
                     CStringComplement((thisRow.GetField(i)->IsNull()) ? "(null)" : thisRow.GetField(i)->GetData(),
                                       DISPLAY_COLUMN_WIDTH));
            }
            printf("\n");
          }
        }
      }
        break;
      case 32: {

        IndexInfo::INDEX_KEY_TYPE32 BeginIndexKey, EndIndexKey;
        if (!(BeginID == INVALID_ROWID)) {
          Row BeginIndexRow = Row(BeginFields);
          BeginIndexKey.SerializeFromKey(BeginIndexRow, __IndexInfo->GetIndexKeySchema());
        }
        if (!(EndID == INVALID_ROWID)) {
          Row EndIndexRow = Row(EndFields);
          EndIndexKey.SerializeFromKey(EndIndexRow, __IndexInfo->GetIndexKeySchema());
        }
        auto CurrentIndexIterator = (BeginID == INVALID_ROWID)
                                        ? __IndexInfo->GetBTreeIndex32()->GetBeginIterator()
                                        : __IndexInfo->GetBTreeIndex32()->GetBeginIterator(BeginIndexKey);
        if (!BoundaryCovered && !(BeginID == INVALID_ROWID)) CurrentIndexIterator++;
        auto IndexEnd = (EndID == INVALID_ROWID) ? __IndexInfo->GetBTreeIndex32()->GetEndIterator()
                                                 : __IndexInfo->GetBTreeIndex32()->GetBeginIterator(EndIndexKey);
        if (BoundaryCovered && !(EndID == INVALID_ROWID)) IndexEnd++;

        // Main Loop
        bool GetReturn;
        for (; CurrentIndexIterator != IndexEnd; ++CurrentIndexIterator) {
          ExecuteContext SelectContext;
          Row thisRow = Row((*CurrentIndexIterator).second);
          GetReturn = __Ti->GetTableHeap()->GetTuple(&thisRow, nullptr);
          if (!GetReturn) {
            printf("Index %s provides wrong RowID when fetching data.\n", __IndexInfo->GetIndexName().c_str());
            return DB_FAILED;
          }
          LogicReturn = LogicConditions(ConditionRoot, &SelectContext, thisRow, __Ti->GetSchema());
          if (LogicReturn != DB_SUCCESS) {
            printf("Failed to analyze logic conditions.\n");
            return DB_FAILED;
          }

          if (SelectContext.condition_) {
            // Select this row:
            ++SelectedRow;
            printf("|");
            // Print columns
            for (auto i : SelectIndexes) {
              printf(" %s |",
                     CStringComplement((thisRow.GetField(i)->IsNull()) ? "(null)" : thisRow.GetField(i)->GetData(),
                                       DISPLAY_COLUMN_WIDTH));
            }
            printf("\n");
          }
        }
      }
        break;
      case 64: {

        IndexInfo::INDEX_KEY_TYPE64 BeginIndexKey, EndIndexKey;
        if (!(BeginID == INVALID_ROWID)) {
          Row BeginIndexRow = Row(BeginFields);
          BeginIndexKey.SerializeFromKey(BeginIndexRow, __IndexInfo->GetIndexKeySchema());
        }
        if (!(EndID == INVALID_ROWID)) {
          Row EndIndexRow = Row(EndFields);
          EndIndexKey.SerializeFromKey(EndIndexRow, __IndexInfo->GetIndexKeySchema());
        }
        auto CurrentIndexIterator = (BeginID == INVALID_ROWID)
                                        ? __IndexInfo->GetBTreeIndex64()->GetBeginIterator()
                                        : __IndexInfo->GetBTreeIndex64()->GetBeginIterator(BeginIndexKey);
        if (!BoundaryCovered && !(BeginID == INVALID_ROWID)) CurrentIndexIterator++;
        auto IndexEnd = (EndID == INVALID_ROWID) ? __IndexInfo->GetBTreeIndex64()->GetEndIterator()
                                                 : __IndexInfo->GetBTreeIndex64()->GetBeginIterator(EndIndexKey);
        if (BoundaryCovered && !(EndID == INVALID_ROWID)) IndexEnd++;

        // Main Loop
        bool GetReturn;
        for (; CurrentIndexIterator != IndexEnd; ++CurrentIndexIterator) {
          ExecuteContext SelectContext;
          Row thisRow = Row((*CurrentIndexIterator).second);
          GetReturn = __Ti->GetTableHeap()->GetTuple(&thisRow, nullptr);
          if (!GetReturn) {
            printf("Index %s provides wrong RowID when fetching data.\n", __IndexInfo->GetIndexName().c_str());
            return DB_FAILED;
          }
          LogicReturn = LogicConditions(ConditionRoot, &SelectContext, thisRow, __Ti->GetSchema());
          if (LogicReturn != DB_SUCCESS) {
            printf("Failed to analyze logic conditions.\n");
            return DB_FAILED;
          }

          if (SelectContext.condition_) {
            // Select this row:
            ++SelectedRow;
            printf("|");
            // Print columns
            for (auto i : SelectIndexes) {
              printf(" %s |",
                     CStringComplement((thisRow.GetField(i)->IsNull()) ? "(null)" : thisRow.GetField(i)->GetData(),
                                       DISPLAY_COLUMN_WIDTH));
            }
            printf("\n");
          }
        }
      }
        break;
      case 128: {

        IndexInfo::INDEX_KEY_TYPE128 BeginIndexKey, EndIndexKey;
        if (!(BeginID == INVALID_ROWID)) {
          Row BeginIndexRow = Row(BeginFields);
          BeginIndexKey.SerializeFromKey(BeginIndexRow, __IndexInfo->GetIndexKeySchema());
        }
        if (!(EndID == INVALID_ROWID)) {
          Row EndIndexRow = Row(EndFields);
          EndIndexKey.SerializeFromKey(EndIndexRow, __IndexInfo->GetIndexKeySchema());
        }
        auto CurrentIndexIterator = (BeginID == INVALID_ROWID)
                                        ? __IndexInfo->GetBTreeIndex128()->GetBeginIterator()
                                        : __IndexInfo->GetBTreeIndex128()->GetBeginIterator(BeginIndexKey);
        if (!BoundaryCovered && !(BeginID == INVALID_ROWID)) CurrentIndexIterator++;
        auto IndexEnd = (EndID == INVALID_ROWID) ? __IndexInfo->GetBTreeIndex128()->GetEndIterator()
                                                 : __IndexInfo->GetBTreeIndex128()->GetBeginIterator(EndIndexKey);
        if (BoundaryCovered && !(EndID == INVALID_ROWID)) IndexEnd++;

        // Main Loop
        bool GetReturn;
        for (; CurrentIndexIterator != IndexEnd; ++CurrentIndexIterator) {
          ExecuteContext SelectContext;
          Row thisRow = Row((*CurrentIndexIterator).second);
          GetReturn = __Ti->GetTableHeap()->GetTuple(&thisRow, nullptr);
          if (!GetReturn) {
            printf("Index %s provides wrong RowID when fetching data.\n", __IndexInfo->GetIndexName().c_str());
            return DB_FAILED;
          }
          LogicReturn = LogicConditions(ConditionRoot, &SelectContext, thisRow, __Ti->GetSchema());
          if (LogicReturn != DB_SUCCESS) {
            printf("Failed to analyze logic conditions.\n");
            return DB_FAILED;
          }

          if (SelectContext.condition_) {
            // Select this row:
            ++SelectedRow;
            printf("|");
            // Print columns
            for (auto i : SelectIndexes) {
              printf(" %s |",
                     CStringComplement((thisRow.GetField(i)->IsNull()) ? "(null)" : thisRow.GetField(i)->GetData(),
                                       DISPLAY_COLUMN_WIDTH));
            }
            printf("\n");
          }
        }
      }
        break;
      default:
        printf("Unexpected index key size (%u).\n", KeySize);
        return DB_FAILED;
    }

  } else {

    // Full Iterate

    auto TableEnd = __Ti->GetTableHeap()->End();
    for (; CurrentIterator != TableEnd; ++CurrentIterator) {
      ExecuteContext SelectContext;
      LogicReturn = LogicConditions(ConditionRoot, &SelectContext, *CurrentIterator, __Ti->GetSchema());
      if (LogicReturn != DB_SUCCESS) {
        printf("Failed to analyze logic conditions.\n");
        return DB_FAILED;
      }

      if (SelectContext.condition_) {
        // Select this row:
        ++SelectedRow;
        printf("|");
        // Print columns
        for (auto i : SelectIndexes) {
          printf(" %s |",
            CStringComplement(
              (CurrentIterator->GetField(i)->IsNull()) ? "(null)" : CurrentIterator->GetField(i)->GetData(),
              DISPLAY_COLUMN_WIDTH));
        }
        printf("\n");
      }
    }

  }

  auto SelectEnd = std::chrono::steady_clock::now();
  // ============== < SELECT END > ==============

  // Print buttom
  printf("+");
  for (uint32_t i = 0; i < SelectColumnNames.size() * (DISPLAY_COLUMN_WIDTH + 3) - 1; ++i) printf("-");
  printf("+\n");
  printf("%u row(s) matched in total.\n", SelectedRow);
  auto SelectTime = std::chrono::duration_cast<std::chrono::duration<double>>(SelectEnd - SelectBegin);
  printf("Time cost: %.2f ms.\n", SelectTime.count() * 1000);

  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteInsert(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "[ExecuteInsert]" << std::endl;
#endif
  if (!strcmp(current_db_.c_str(), "")) {
    printf("Never specified any database yet.\n");
    return DB_FAILED;
  }

  // Get parameter table_name
  auto astIden = ast->child_;
  if (astIden->type_ != kNodeIdentifier) {
    printf("Invalid identifier.\n");
    return DB_FAILED;
  }
  auto TableName = std::string(astIden->val_);
  TableInfo *__Ti;
  dberr_t GetTableReturn = dbs_[current_db_]->catalog_mgr_->GetTable(TableName, __Ti);
  if (GetTableReturn != DB_SUCCESS) {
    printf("Table %s not found!\n", TableName.c_str());
    return DB_FAILED;
  }
  auto TableColumns = __Ti->GetSchema()->GetColumns();
  
  // Construct row
  std::vector<Field> __Fields;
  if (ast->child_->next_->type_ == kNodeColumnValues) {

    uint32_t ValueCount = 0;
    TypeId ColumnType;
    auto astValue = ast->child_->next_->child_;
    // Get data from linked list
    while (ValueCount < TableColumns.size()) {

      if (astValue) {

        // You cannot insert NULL to not null column
        if (astValue->type_ == kNodeNull && !(TableColumns[ValueCount]->IsNullable())) {
          printf("Cannot insert NULL into a not nullable column.\n");
          return DB_FAILED;
        }

        ColumnType = TableColumns[ValueCount]->GetType();
        bool WrongType = false;
        switch (astValue->type_) {
          case kNodeNumber:
            if (ColumnType == kTypeInt) {
              if (isFloat(astValue->val_)) 
                WrongType = true;
              else
                __Fields.push_back(Field(kTypeInt, atoi(astValue->val_)));
            } else if (ColumnType == kTypeFloat) {
              __Fields.push_back(Field(kTypeFloat, static_cast<float>(atof(astValue->val_))));
            } else {
              WrongType = true;
            }
            break;
          case kNodeString:
            if (ColumnType != kTypeChar) WrongType = true;
            __Fields.push_back(Field(
              kTypeChar, 
              astValue->val_,
              __Ti->GetSchema()->GetColumn(__Fields.size())->GetLength(),
              true
            ));  // manage_data: true => deep_copy
            break;
          case kNodeNull:
            __Fields.push_back(Field(__Ti->GetSchema()->GetColumn(__Fields.size())->GetType()));  // Insert NULL
            break;
          default:
            printf("Unexpected column value type.\n");
            return DB_FAILED;
        }
        
        if (WrongType) {
          printf("Value %u has a type different from table schema!\n", ValueCount);
          return DB_FAILED;
        }

      } else {
        printf("Too few values to insert.\n");
        return DB_FAILED;
      }
      
      ++ValueCount;
      astValue = astValue->next_;       // Next value
    }

    if (astValue) {
      printf("Too many values to insert. Please check out statements and try again.\n");
      return DB_FAILED;
    }

  } else {
    printf("Unexpected node type.\n");
    return DB_FAILED;
  }
  
  Row row = Row(__Fields);
  bool InsertReturn = __Ti->GetTableHeap()->InsertTuple(row, nullptr);
  if (!InsertReturn) {
    printf("Insert error.\n");
    return DB_FAILED;
  }

  RowId row_id = row.GetRowId();    // RowId
  // Insert new row to indexes
  dberr_t InsertEntryReturn;
  std::vector<Field> IndexFields;
  std::vector<Column *> IndexColumns;
  std::vector<IndexInfo *> TableIndexes;
  dbs_[current_db_]->catalog_mgr_->GetTableIndexes(__Ti->GetTableName(), TableIndexes);
  // For a moment I flash a thought of checking the return of GetTableIndexes
  // But recently I realize ... the only thing you can give ... is DB_SUCCESS ...

  for (auto __Idx : TableIndexes) {

    // Get index columns and iterate
    IndexColumns = __Idx->GetIndexKeySchema()->GetColumns();
    IndexFields.clear();
    for (auto __Col : IndexColumns) {
      IndexFields.push_back(__Fields[__Col->GetTableInd()]);
    }

    // Insert into index
    Row NewIndexRow = Row(IndexFields);
    InsertEntryReturn = 
      __Idx->GetIndex()->InsertEntry(NewIndexRow, row_id, nullptr);
    if (InsertEntryReturn != DB_SUCCESS) {
      printf("Index insert error. You may insert duplicate value into a unique column.\n");
      __Ti->GetTableHeap()->MarkDelete(row_id, nullptr);
      return DB_FAILED;
    }
  }

  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDelete(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "[ExecuteDelete]" << std::endl;
#endif
  if (!strcmp(current_db_.c_str(), "")) {
    printf("Never specified any database yet.\n");
    return DB_FAILED;
  }

  // Get parameter table_name
  auto astIden = ast->child_;
  if (astIden->type_ != kNodeIdentifier) {
    printf("Invalid identifier.\n");
    return DB_FAILED;
  }
  auto TableName = std::string(astIden->val_);
  TableInfo *__Ti;
  dberr_t GetTableReturn =
    dbs_[current_db_]->catalog_mgr_->GetTable(TableName, __Ti);
  if (GetTableReturn != DB_SUCCESS) {
    printf("Table %s not found!\n", TableName.c_str());
    return DB_FAILED;
  }
  auto CurrentIterator = __Ti->GetTableHeap()->Begin(nullptr);

  bool DelectReturn;
  auto ConditionRoot = ast->child_->next_;

  // DELECT BEGIN
  dberr_t LogicReturn;
  auto TableEnd = __Ti->GetTableHeap()->End();
  // For indexes
  std::vector<Field> __FieldsToDelete;
  std::vector<Column *> IndexColumns;
  std::vector<IndexInfo *> TableIndexes;
  dbs_[current_db_]->catalog_mgr_->GetTableIndexes(TableName, TableIndexes);

  for (; CurrentIterator != TableEnd; ++CurrentIterator) {
    ExecuteContext SelectContext;
    LogicReturn = LogicConditions(ConditionRoot, &SelectContext, *CurrentIterator, __Ti->GetSchema());
    if (LogicReturn != DB_SUCCESS) {
      printf("Failed to analyze logic conditions.\n");
      return DB_FAILED;
    }
    if (SelectContext.condition_) {
      // Select this row:
      auto __DeletedRow = *CurrentIterator;
      DelectReturn = __Ti->GetTableHeap()->MarkDelete(CurrentIterator->GetRowId(), nullptr);
      if (!DelectReturn) {
        printf("Failed to delete tuple. (RowId = %ld)\n", CurrentIterator->GetRowId().Get());
        continue;
      }
 
      // Delete in indexes
      for (auto __Idx : TableIndexes) {
        IndexColumns.clear();
        IndexColumns = __Idx->GetIndexKeySchema()->GetColumns();
        for (auto __Col : IndexColumns) 
          __FieldsToDelete.push_back(*( __DeletedRow.GetField(__Col->GetTableInd())));
        Row RowToDelete = Row(__FieldsToDelete);
        __Idx->GetIndex()->RemoveEntry(RowToDelete, __DeletedRow.GetRowId(), nullptr);      // You! You can only return SUCCESS too!
      }
    }
  }
  // DELECT END

  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteUpdate(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "[ExecuteUpdate]" << std::endl;
#endif
  if (!strcmp(current_db_.c_str(), "")) {
    printf("Never specified any database yet.\n");
    return DB_FAILED;
  }

  TableInfo *__Ti;
  dberr_t GetTableReturn =
    dbs_[current_db_]->catalog_mgr_->GetTable(ast->child_->val_, __Ti);
  if (GetTableReturn != DB_SUCCESS) {
    printf("Table %s not found!\n", ast->child_->val_);
    return DB_FAILED;
  }

  auto UpdateTableName = std::string(ast->child_->val_);
  auto CurrentIterator = __Ti->GetTableHeap()->Begin(nullptr);

  std::vector<std::string> UpdateColumnNames;
  std::vector<Field> UpdateFieldList;
  if (ast->child_->next_->type_ == kNodeUpdateValues) {
    auto astCol = ast->child_->next_->child_;
    pSyntaxNode astValue;
    while (astCol) {
      UpdateColumnNames.push_back(std::string(astCol->child_->val_));
      
      // New update field
      astValue = astCol->child_->next_;
      switch (astValue->type_) {
        case kNodeNumber:
          if (isFloat(astValue->val_)) {
            UpdateFieldList.push_back(Field(kTypeFloat, (float)atof(astValue->val_)));
          } else {
            UpdateFieldList.push_back(Field(kTypeInt, atoi(astValue->val_)));
          }
          break;
        case kNodeString:
          UpdateFieldList.push_back(Field(kTypeChar, astValue->val_,
            __Ti->GetSchema()->GetColumn(UpdateFieldList.size())->GetLength(),true));           // manage_data: true => deep_copy
          break;
        case kNodeNull:
          UpdateFieldList.push_back(
            Field(__Ti->GetSchema()->GetColumn(UpdateFieldList.size())->GetType()));            // Insert NULL
          break;
        default:
          printf("Unexpected column value type.\n");
          return DB_FAILED;
      }

      astCol = astCol->next_;  // Next column
    }
  } else {
    printf("Unexpected columns!.\n");
    return DB_FAILED;
  }

  // Generate index array
  dberr_t FindColumnReturn;
  uint32_t __Idx;  // Temp
  std::vector<uint32_t> UpdateIndexes;
  // Find ..
  for (auto ColumnStr : UpdateColumnNames) {
    FindColumnReturn = __Ti->GetSchema()->GetColumnIndex(ColumnStr, __Idx);
    if (FindColumnReturn != DB_SUCCESS) {
      printf("Column %s not found!\n", ColumnStr.c_str());
      return DB_FAILED;
    }
    UpdateIndexes.push_back(__Idx);
  }

  auto ConditionRoot = ast->child_->next_->next_;

  // UPDATE
  uint32_t Updated = 0;
  bool UpdateReturn;
  dberr_t LogicReturn;
  auto TableEnd = __Ti->GetTableHeap()->End();
  std::vector<Field> UpdateFields;
  std::vector<Field *> OldFields;
  for (; CurrentIterator != TableEnd; ++CurrentIterator) {
    ExecuteContext SelectContext;
    LogicReturn = LogicConditions(ConditionRoot, &SelectContext, *CurrentIterator, __Ti->GetSchema());
    if (LogicReturn != DB_SUCCESS) {
      printf("Failed to analyze logic conditions.\n");
      return DB_FAILED;
    }

    if (SelectContext.condition_) {
      // Select this row:
      // Generate old row
      UpdateFields.clear();
      OldFields = CurrentIterator->GetFields();
      for (auto f : OldFields) UpdateFields.push_back(Field(*f));
      // Update New row
      for (long unsigned int i = 0; i < UpdateIndexes.size(); ++i) {
        UpdateFields[UpdateIndexes[i]] = *(new Field(UpdateFieldList[i]));
      }
      Row __newrow = Row(UpdateFields);
      // Update
      RowId __rowid = CurrentIterator->GetRowId();
      UpdateReturn = __Ti->GetTableHeap()->UpdateTuple(__newrow, __rowid, nullptr);
      if (!UpdateReturn) printf("Failed to update row (rowid = %ld).\n", __rowid.Get());
      ++Updated;
    }
  }
  // UPDATE END

  printf("%u row(s) updated in total.\n", Updated);

  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "[ExecuteTrxBegin]" << std::endl;
#endif
  __Unflod__SyntaxTree(ast, 0);
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "[ExecuteTrxCommit]" << std::endl;
#endif
  __Unflod__SyntaxTree(ast, 0);
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "[ExecuteTrxRollback]" << std::endl;
#endif
  __Unflod__SyntaxTree(ast, 0);
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "[ExecuteExecfile]" << std::endl;
#endif
  char ExecuteCommand[1024];
  ifstream ExecuteFile(ast->child_->val_);
  
  if (ExecuteFile.is_open()) {

    char ch;
    int i;
    while (!ExecuteFile.eof()) {

      // "I think you just copy from main.cpp without doubt."
      // "You are right."
      i = 0;
      while ((ch = ExecuteFile.get()) != ';') {
        if (ch == EOF) {
          printf("Unexpected end of execute file.\n");
          return DB_FAILED;
        }
        ExecuteCommand[i++] = ch;
      }
      ExecuteCommand[i] = ch;       // ;
      ExecuteCommand[i + 1] = '\0'; // You can not image that how pageantry bugs this stupid mistake cause ..
      ExecuteFile.get();            // remove enter
      printf("%s\n", ExecuteCommand);
      
      // create buffer for sql input
      YY_BUFFER_STATE bp = yy_scan_string(ExecuteCommand);
      if (bp == nullptr) {
        LOG(ERROR) << "Failed to create yy buffer state when execute from file." << std::endl;
        return DB_FAILED;
      }
      yy_switch_to_buffer(bp);

      // init parser module
      MinisqlParserInit();
      // parse
      yyparse();
      if (MinisqlParserGetError()) {
        // error
        printf("%s\n", MinisqlParserGetErrorMessage());
      } else {
#ifdef ENABLE_PARSER_DEBUG
        printf("[INFO] Sql syntax parse ok!\n");
        SyntaxTreePrinter printer(MinisqlGetParserRootNode());
        printer.PrintTree(syntax_tree_file_mgr[syntax_tree_id++]);
#endif
      }

      // EXECUTE
      ExecuteContext ExecfileContext;
      Execute(MinisqlGetParserRootNode(), &ExecfileContext);

      // clean memory after parse
      MinisqlParserFinish();
      yy_delete_buffer(bp);
      yylex_destroy();

      // quit condition
      if (ExecfileContext.flag_quit_) {
        context->flag_quit_ = true;
        break;
      }
    }

  } else {
    printf("Failed to load execute file from disk.\n");
    return DB_FAILED;
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "[ExecuteQuit]" << std::endl;
#endif
  ASSERT(ast->type_ == kNodeQuit, "Unexpected node type.");
  context->flag_quit_ = true;
  return DB_SUCCESS;
}

// ============ < OPERATOR > ============

/* Operator functions used in SELECT, UPDATE and DELETE.
*  All operations are based on syntax tree and read data
*  from table interface.
*/

dberr_t ExecuteEngine::LogicConditions(pSyntaxNode ast, ExecuteContext *context, const Row &row, Schema *schema) {
  
  if (context->flag_quit_) return DB_SUCCESS;
  if (ast == nullptr) {
    // Consider NULL as true
    context->condition_ = true;
    return DB_SUCCESS;
  }

  if (ast->type_ == kNodeConditions) {

    LogicConditions(ast->child_, context, row, schema);

  } else if (ast->type_ == kNodeConnector) {

    ExecuteContext LeftExpression, RightExpression;     // condition_ is initialized true.
    LogicConditions(ast->child_, &LeftExpression, row, schema);

    if (strcmp(ast->val_, "and") == 0) {
      
      // Operator and
      if (LeftExpression.condition_) {      // Optimization: False && A = False
        LogicConditions(ast->child_->next_, &RightExpression, row, schema);
        context->condition_ = RightExpression.condition_;
      } else {
        context->condition_ = false;
      }

    } else if (strcmp(ast->val_, "or") == 0) {
      
      // Operator or
      if (!LeftExpression.condition_) {     // Optimization: True || A = True
        LogicConditions(ast->child_->next_, &RightExpression, row, schema);
        context->condition_ = RightExpression.condition_;
      } else {
        //context->condition_ = true;
        // In fact you can do nothing as context->condition_ has been initialized true.
      }

    } else {
      printf("Unexpected logic connector.\n");
      context->flag_quit_ = true;
      return DB_FAILED;
    }

  } else if (ast->type_ == kNodeCompareOperator) {

    // True (by Iterator Selector)
    if (strcmp(ast->val_, "TRUE") == 0) {
      context->condition_ = true;
      return DB_SUCCESS;
    }

    // Take data
    bool LeftNull = false, RightNull = false;
    std::string LeftValueStr, RightValueStr;

    // Left
    switch (ast->child_->type_) {
      case kNodeIdentifier:
        uint32_t IdentifierIndex;
        schema->GetColumnIndex(ast->child_->val_, IdentifierIndex);
        if (row.GetField(IdentifierIndex)->IsNull())
          LeftNull = true;
        else
          LeftValueStr = row.GetField(IdentifierIndex)->GetData();
        break;
      case kNodeNumber:
      case kNodeString:
        LeftValueStr = ast->child_->val_;
        break;
      case kNodeNull:
        LeftNull = true;
        break;
      default:
        return DB_FAILED;
    }

    // Right
    switch (ast->child_->next_->type_) {
      case kNodeIdentifier:
        uint32_t IdentifierIndex;
        schema->GetColumnIndex(ast->child_->next_->val_, IdentifierIndex);
        if (row.GetField(IdentifierIndex)->IsNull())
          RightNull = true;
        else
          RightValueStr = row.GetField(IdentifierIndex)->GetData();
        break;
      case kNodeNumber:
      case kNodeString:
        RightValueStr = ast->child_->next_->val_;
        break;
      case kNodeNull:
        RightNull = true;
        break;
      default:
        return DB_FAILED;
    }

    // Comparation ..
    if (strcmp(ast->val_, "is") == 0) {
      context->condition_ = (LeftNull && RightNull);
    } else if (strcmp(ast->val_, "not") == 0) {
      context->condition_ = ((!LeftNull) && RightNull);
    } else if (!(LeftNull || RightNull)) {   // Null value can not use these comparation operators
      
      if (strcmp(ast->val_, "<=") == 0) {
        context->condition_ = (atof(LeftValueStr.c_str()) <= atof(RightValueStr.c_str()));
      } else if (strcmp(ast->val_, ">=") == 0) {
        context->condition_ = (atof(LeftValueStr.c_str()) >= atof(RightValueStr.c_str()));
      } else if (strcmp(ast->val_, "<") == 0) {
        context->condition_ = (atof(LeftValueStr.c_str()) < atof(RightValueStr.c_str()));
      } else if (strcmp(ast->val_, ">") == 0) {
        context->condition_ = (atof(LeftValueStr.c_str()) > atof(RightValueStr.c_str()));
      } else if (strcmp(ast->val_, "<>") == 0 || strcmp(ast->val_, "!=") == 0) {
        context->condition_ = (strcmp(LeftValueStr.c_str(), RightValueStr.c_str()) != 0);
      } else if (strcmp(ast->val_, "=") == 0) {
        context->condition_ = (strcmp(LeftValueStr.c_str(), RightValueStr.c_str()) == 0);
      } else {
        printf("Unexpected comparison operator.\n");
        context->flag_quit_ = true;
        return DB_FAILED;
      }

    } else {
      printf("Null value can only use \"is null\" or \"not null\" to compare.\n");
      context->flag_quit_ = true;
      return DB_FAILED;
    }

  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::SelectIterator(pSyntaxNode condition, ExecuteContext *context, const TableInfo *table,
                                      IndexInfo *&index, RowId &begin_id, RowId &end_id, bool &covered) {
  if (condition == nullptr) {
    return DB_SUCCESS;
  }

  if (context->index_ind >= 0) {
    // Have found a index
    return DB_SUCCESS;
  }

  if (condition->type_ == kNodeConditions) {

    SelectIterator(condition->child_, context, table, index, begin_id, end_id, covered);

  } else if (condition->type_ == kNodeConnector) {

    if (strcmp(condition->val_, "and") == 0) {
      SelectIterator(condition->child_, context, table, index, begin_id, end_id, covered);
      SelectIterator(condition->child_->next_, context, table, index, begin_id, end_id, covered);
    }

  } else if (condition->type_ == kNodeCompareOperator) {

    const uint32_t NO_IDENTIFIER = 23333333;  // It is not an identifier
    uint32_t LeftIndex = NO_IDENTIFIER, RightIndex = NO_IDENTIFIER;

    if (strcmp(condition->val_, "<>") != 0 && strcmp(condition->val_, "!=") != 0) {
      // Choose this identifier
      if (condition->child_->next_->type_ == kNodeIdentifier)
        table->GetSchema()->GetColumnIndex(std::string(condition->child_->next_->val_), RightIndex);
      if (condition->child_->type_ == kNodeIdentifier)
        table->GetSchema()->GetColumnIndex(std::string(condition->child_->val_), LeftIndex);
      if (LeftIndex == NO_IDENTIFIER && RightIndex != NO_IDENTIFIER) context->index_ind = RightIndex;
      if (LeftIndex != NO_IDENTIFIER && RightIndex == NO_IDENTIFIER) context->index_ind = LeftIndex;
    }

    if (context->index_ind >= 0) {
      // Get IndexInfo
      bool founded = false;
      uint32_t IndexKeyIndex;
      std::vector<IndexInfo *> TableIndexes;
      dbs_[current_db_]->catalog_mgr_->GetTableIndexes(table->GetTableName(), TableIndexes);
      for (auto __Idx : TableIndexes) {
        table->GetSchema()->GetColumnIndex(__Idx->GetIndexKeySchema()->GetColumns()[0]->GetName(), IndexKeyIndex);
        if (static_cast<int>(__Idx->GetIndexKeySchema()->GetColumns().size()) == 1 &&
            static_cast<int>(IndexKeyIndex) == context->index_ind) {
          // Choose this index!
          index = __Idx;
          founded = true;
          break;
        }
      }
      if (!founded) {
        context->index_ind = -1;
        return DB_SUCCESS;
      }

      // Find RowId of boundary condition value
      auto BoundaryValue = (context->index_ind == static_cast<int>(LeftIndex)) ? condition->child_->next_->val_
                                                                               : condition->child_->val_;
      std::vector<Field> BoundaryField;
      switch (index->GetIndexKeySchema()->GetColumns()[0]->GetType()) {
        case kTypeInt:
          BoundaryField.push_back(Field(kTypeInt, atoi(BoundaryValue)));
          break;
        case kTypeFloat:
          BoundaryField.push_back(Field(kTypeFloat, static_cast<float>(atof(BoundaryValue))));
          break;
        case kTypeChar:
          BoundaryField.push_back(
              Field(kTypeChar, BoundaryValue, table->GetSchema()->GetColumns()[context->index_ind]->GetLength(), true));
          break;
        default:
          printf("Comparation \"%s %s %s\" cannot match the schema of index %s.\n", condition->child_->val_,
                 condition->val_, condition->child_->next_->val_, index->GetIndexName().c_str());
          context->index_ind = -1;
          return DB_FAILED;
      }
      std::vector<RowId> ScanKeyResult;
      dberr_t ScanReturn = index->GetIndex()->ScanKey(Row(BoundaryField), ScanKeyResult, nullptr);
      if (ScanReturn != DB_SUCCESS) {
        printf("Value %s does not exist in index %s.\n", BoundaryValue, index->GetIndexName().c_str());
        context->index_ind = -1;
        return DB_FAILED;
      }

      // Set index search range
      if (strcmp(condition->val_, "=") == 0) {
        begin_id = end_id = ScanKeyResult[0];
        covered = true;
      } else if (strcmp(condition->val_, "<=") == 0) {
        begin_id = INVALID_ROWID, end_id = ScanKeyResult[0];
        covered = true;
      } else if (strcmp(condition->val_, ">=") == 0) {
        begin_id = ScanKeyResult[0], end_id = INVALID_ROWID;
        covered = true;
      } else if (strcmp(condition->val_, "<") == 0) {
        begin_id = INVALID_ROWID, end_id = ScanKeyResult[0];
        covered = false;
      } else if (strcmp(condition->val_, ">") == 0) {
        begin_id = ScanKeyResult[0], end_id = INVALID_ROWID;
        covered = false;
      } else {
        printf("Unexpected comparison operator.\n");
        context->index_ind = -1;
        return DB_FAILED;
      }

      // Set condition to always true
      strcpy(condition->val_, "TRUE");
    }

  } else {
    return DB_FAILED;
  }

  return DB_SUCCESS;
}