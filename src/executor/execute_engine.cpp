#include <cstdio>
#include <fstream>
#include "executor/execute_engine.h"
#include "glog/logging.h"

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

/* Tyoe cast from c_str to int
*/
int CStrToInt(const char *cstr) {
  bool sign = (cstr[0] == '-');
  int ret = 0;
  for (int i = 1; cstr[i] != '\0'; ++i) {
    ret *= 10;
    ret += cstr[i] - '0';
  }
  if (sign) ret = -ret;
  return ret;
}

/* Tyoe cast from c_str to float
 */
float CStrToFloat(const char *cstr) {
  bool sign = (cstr[0] == '-');
  float ret = 0;
  // Before point
  int i = 1;
  for (; cstr[i] >= '0' && cstr[i] <= '9'; ++i) {
    ret *= 10;
    ret += cstr[i] - '0';
  }
  // Check point
  if (cstr[i] == '\0') {
    if (sign) ret = -ret;
    return ret;
  }
  // After point
  float factor = 0.1;
  for (i = i + 1; cstr[i] != '\0'; ++i) {
    ret += (cstr[i] - '0') * factor;
  }
  if (sign) ret = -ret;
  return ret;
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
  printf("%ld database(s) available now.\n", dbs_.size());
  for (auto p : dbs_) {
    printf(" %s\n", p.first.c_str());
  }
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

  current_db_ = std::string(astIden->val_);
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
    printf("%ld table(s) in database %s.\n", TableList.size(), current_db_.c_str());
    for (auto T : TableList) {
      printf(" %s\n", T->GetTableName().c_str());
    }
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
  auto NewTableIdentifier = std::string(astIden->val_);  // Identifier

  // Get column definition list
  if (astIden->next_ && astIden->next_->type_ == kNodeColumnDefinitionList &&
      astIden->next_->child_) {
    uint32_t __Index = 0;
    auto astCol = astIden->next_->child_;
    std::vector<Column *> ColumnList;

    // Get parameters
    while (astCol) {

      if (astCol->type_ == kNodeColumnDefinition) {
        // Crisis check (TO DO)
        // bool IdentifierCollided = false;
        // Insertion
        TypeId NewColumnType = GetTypeId(astCol->child_->next_->val_);
        if (NewColumnType == kTypeChar) {
          ColumnList.push_back(new Column(
            std::string(astCol->child_->val_),  // Name
            NewColumnType,                      // Type
            atoi(astCol->child_->next_->child_->val_),  // Length
            __Index++,                          // Index, start from 0
            true,                               // Nullable
            false                               // Unique
          ));  // TO DO: find out whether the syntax tree process "nullable" and "unique"
        } else {
          ColumnList.push_back(new Column(
            std::string(astCol->child_->val_),  // Name
            NewColumnType,                      // Type
            __Index++,                          // Index, start from 0
            true,                               // Nullable
            false                               // Unique
          ));  // TO DO: find out whether the syntax tree process "nullable" and "unique"
        }
      
      } else if (astCol->type_ == kNodeColumnList) {
        
        auto astPrimaryIden = astCol->child_;
        while (astPrimaryIden) {
          // Find key by name
          long unsigned int KeyIndex = 0;
          for (; KeyIndex < ColumnList.size(); ++KeyIndex) {
            if (string(astPrimaryIden->val_) == ColumnList[KeyIndex]->GetName()) {
              break;
            }
          }
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

    TableSchema *NewTableSchema = new Schema(ColumnList);   // Create Schema
    TableInfo *NewTableInfo;                                // Create TableInfo
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
          
          printf(" %s on %s(", Ip->GetIndexName().c_str(), Tp->GetTableName().c_str());
          for (auto Cp : Ip->GetIndexKeySchema()->GetColumns()) {
            // For each column in index Ip:
            if (Cp == (Ip->GetIndexKeySchema()->GetColumns())[0])
              printf("%s", Cp->GetName().c_str());
            else
              printf(",%s", Cp->GetName().c_str());
            // I'm nearly dizzy #$^@(%SD^(V)@%@^+:+@!%_^
          }
          printf(")\n");
        }
      } else {
        printf("Failed when search indexes from table %s.\n", Tp->GetTableName().c_str());
      }
    }

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

  // Para 3: Column list
  astIden = astIden->next_;
  auto astCol = astIden->child_;    // Column Identifiers
  std::vector<std::string> NewIndexColumns;
  while (astCol) {
    NewIndexColumns.push_back(std::string(astCol->val_));
    astCol = astCol->next_;     // Next identifier
  }

  // Para 4: Index type (optional)
  //astIden = ast->next_;
  //if (astIden) Change index type;
  
  IndexInfo *NewIndexInfo;
  dberr_t CreateReturn = 
    dbs_[current_db_]->catalog_mgr_->
    CreateIndex(TableName, NewIndexName, NewIndexColumns, nullptr, NewIndexInfo);
  if (CreateReturn != DB_SUCCESS) {
    printf("Failed to create index in table %s.\n", TableName.c_str());
    return DB_FAILED;
  }
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

  if (GetReturn == DB_SUCCESS) {

    bool IndexFound = false;
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
      }
      if (IndexFound) break;
    }
    
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
    printf("Unexpected columns!.\n");
    return DB_FAILED;
  }
  
  TableInfo *__Ti;
  dbs_[current_db_]->catalog_mgr_->GetTable(ast->child_->next_->val_, __Ti);
  auto CurrentIterator = __Ti->GetTableHeap()->Begin(nullptr);

  // Generate index array
  dberr_t FindColumnReturn;
  uint32_t __Idx;   // Temp
  std::vector<uint32_t> SelectIndexes;
  // Find ..
  if (SelectColumnNames.size() > 0) {

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

  // Print title
  for (uint32_t i = 0; i < SelectColumnNames.size() * 12 + 2; ++i) printf("=");
  printf("\n");
  for (auto __Str : SelectColumnNames) printf("%12s", __Str.c_str());
  printf("\n");

  auto ConditionRoot = ast->child_->next_->next_;
  if (ConditionRoot && ConditionRoot->type_ == kNodeConditions) {

    ConditionRoot = ConditionRoot->child_;

    // SELECT
    dberr_t LogicReturn; 
    auto TableEnd = __Ti->GetTableHeap()->End();
    for (; CurrentIterator != TableEnd; ++CurrentIterator) {
      
      ExecuteContext SelectContext;
      LogicReturn =
        LogicConditions(ConditionRoot, &SelectContext, *CurrentIterator, __Ti->GetSchema());
      if (LogicReturn != DB_SUCCESS) {
        printf("Failed to analyze logic conditions.\n");
        return DB_FAILED;
      }

      if (SelectContext.condition_) {

        // Select this row:
        for (auto i : SelectIndexes) {
          printf("%12s", CurrentIterator->GetField(i)->GetData());
        }
        printf("\n");
      }
    }
    // SELECT END

  } else {

    // SELECT WITHOUT ANY CONDITION
    
    auto TableEnd = __Ti->GetTableHeap()->End();
    for (; CurrentIterator != TableEnd; ++CurrentIterator) {
      
      for (auto i : SelectIndexes) {
        printf("%12s", CurrentIterator->GetField(i)->GetData());
      }
      printf("\n");
    }
    // SELECT END
  }

  // Print buttom
  for (uint32_t i = 0; i < SelectColumnNames.size() * 12 + 2; ++i) printf("=");
  printf("\n");

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
  dbs_[current_db_]->catalog_mgr_->GetTable(TableName, __Ti);
  
  // Construct row
  std::vector<Field> __Fields;
  if (ast->child_->next_->type_ == kNodeColumnValues) {

    auto astValue = ast->child_->next_->child_;
    // Get data from linked list
    while (astValue) {

      switch (astValue->type_) {
        case kNodeNumber:
          if (isFloat(astValue->val_)) {
            __Fields.push_back(Field(kTypeFloat, (float)atof(astValue->val_)));
          } else {
            __Fields.push_back(Field(kTypeInt, atoi(astValue->val_)));
          }
          break;
        case kNodeString:
          __Fields.push_back(Field(kTypeChar, astValue->val_, 
            __Ti->GetSchema()->GetColumn(__Fields.size())->GetLength(), true));                 // manage_data: true => deep_copy
          break;
        case kNodeNull:
          __Fields.push_back(Field(__Ti->GetSchema()->GetColumn(__Fields.size())->GetType()));  // Insert NULL
          break;
        default:
          printf("Unexpected column value type.\n");
          return DB_FAILED;
      }
      astValue = astValue->next_;       // Next value
    }

  } else {
    printf("Unexpected node type.\n");
    return DB_FAILED;
  }
  
  Row row = Row(__Fields);
  bool InsertReturn =
    __Ti->GetTableHeap()->InsertTuple(row, nullptr);

  if (!InsertReturn) {
    printf("Insert error.\n");
    return DB_FAILED;
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
  dbs_[current_db_]->catalog_mgr_->GetTable(TableName, __Ti);
  auto CurrentIterator = __Ti->GetTableHeap()->Begin(nullptr);

  bool DelectReturn;
  auto ConditionRoot = ast->child_->next_;
  if (ConditionRoot && ConditionRoot->type_ == kNodeConditions) {
    ConditionRoot = ConditionRoot->child_;

    // DELECT
    dberr_t LogicReturn;
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
        DelectReturn =
          __Ti->GetTableHeap()->MarkDelete(CurrentIterator->GetRowId(), nullptr);
        if (!DelectReturn) 
          printf("Failed to delete tuple. (RowId = %ld)\n", CurrentIterator->GetRowId().Get());
      }
    }
    // DELECT END

  } else {
    // DELECT WITHOUT ANY CONDITION
    auto TableEnd = __Ti->GetTableHeap()->End();
    for (; CurrentIterator != TableEnd; ++CurrentIterator) {
      DelectReturn =
        __Ti->GetTableHeap()->MarkDelete(CurrentIterator->GetRowId(), nullptr);
      if (!DelectReturn)
        printf("Failed to delete tuple. (RowId = %ld)\n", CurrentIterator->GetRowId().Get());
    }
    // DELECT END
  }

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
  dbs_[current_db_]->catalog_mgr_->GetTable(ast->child_->val_, __Ti);
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
  if (ConditionRoot && ConditionRoot->type_ == kNodeConditions) {
    ConditionRoot = ConditionRoot->child_;

    // UPDATE
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
          UpdateFields[UpdateIndexes[i]] = UpdateFieldList[i];
        }
        Row __newrod = Row(UpdateFields);
        // Update
        RowId __rowid = CurrentIterator->GetRowId();
        __Ti->GetTableHeap()->UpdateTuple(__newrod, __rowid, nullptr);
      }
    }
    // UPDATE END

  } else {
    // UPDATE WITHOUT ANY CONDITION
    auto TableEnd = __Ti->GetTableHeap()->End();
    std::vector<Field> UpdateFields;
    std::vector<Field *> OldFields;
    for (; CurrentIterator != TableEnd; ++CurrentIterator) {

      // Select this row:
      // Generate old row
      UpdateFields.clear();
      OldFields = CurrentIterator->GetFields();
      for (auto f : OldFields) UpdateFields.push_back(Field(*f));
      // Update New row
      for (long unsigned int i = 0; i < UpdateIndexes.size(); ++i) {
          UpdateFields[UpdateIndexes[i]] = UpdateFieldList[i];
      }
      Row __newrod = Row(UpdateFields);
      // Update
      RowId __rowid = CurrentIterator->GetRowId();
      __Ti->GetTableHeap()->UpdateTuple(__newrod, __rowid, nullptr);
    }
    // UPDATE END
  }

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
  
  if (ast->type_ == kNodeConnector) {

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

    return DB_SUCCESS;

  } else if (ast->type_ == kNodeCompareOperator) {

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
    if (strcmp(ast->val_, "is")) {
      context->condition_ = (LeftNull && RightNull);
    } else if (strcmp(ast->val_, "not")) {
      context->condition_ = (LeftNull != RightNull);
    } else if (!LeftNull && !RightNull) {   // Null value can not use these comparation operators
      
      if (strcmp(ast->val_, "<=")) {
        context->condition_ = (atof(LeftValueStr.c_str()) <= atof(RightValueStr.c_str()));
      } else if (strcmp(ast->val_, ">=")) {
        context->condition_ = (atof(LeftValueStr.c_str()) >= atof(RightValueStr.c_str()));
      } else if (strcmp(ast->val_, "<")) {
        context->condition_ = (atof(LeftValueStr.c_str()) < atof(RightValueStr.c_str()));
      } else if (strcmp(ast->val_, ">")) {
        context->condition_ = (atof(LeftValueStr.c_str()) > atof(RightValueStr.c_str()));
      } else if (strcmp(ast->val_, "=")) {
        context->condition_ = (LeftValueStr.c_str() == RightValueStr.c_str());
      } else {
        printf("Unexpected comparison operator.\n");
        context->flag_quit_ = true;
        return DB_FAILED;
      }

    } else {
      printf("Cannot compare null value.\n");
      context->flag_quit_ = true;
      return DB_FAILED;
    }

    // Finish!
    return DB_SUCCESS;
  }
  return DB_FAILED;
}