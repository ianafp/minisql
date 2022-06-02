## CATALOG MANAGER实验报告

***

姓名： 张鑫

学号：3200102809

专业：计算机科学与技术

### 0 绪论

***

本模块由我一人完成。Catalog Manager 负责管理和维护数据库的所有模式信息，包括：

- 数据库中所有表的定义信息，包括表的名称、表中字段（列）数、主键、定义在该表上的索引。
- 表中每个字段的定义信息，包括字段类型、是否唯一等。
- 数据库中所有索引的定义，包括所属表、索引建立在那个字段上等。

这些模式信息在被创建、修改和删除后还应被持久化到数据库文件中。此外，Catalog Manager还需要为上层的执行器Executor提供公共接口以供执行器获取目录信息并生成执行计划。

### 1 序列化

***

#### 1.1 Catalog Meta

***

Catalog Meta存储着数据库系统的元数据，诸如，表id到表meta页，索引id到索引meta页的映射。

Catalog Meta的成员如下：

```c++
  static constexpr uint32_t CATALOG_METADATA_MAGIC_NUM = 89849;
  std::map<table_id_t, page_id_t> table_meta_pages_;
  std::map<index_id_t, page_id_t> index_meta_pages_;
```

meta data中表和索引的映射是需要进行持久化存储的。

因而对meta data的序列化次序为： MAGIC_NUM -> size of (table_meta_pages) -> size of(index_meta_pages) -> 遍历table_meta_pages序列化 -> 遍历index_meta_pages_序列化。

#### 1.2 Table Meta

***

Table Meta中存储的是对应着一张表的持久化数据，包括表的id，表的名字，表对应的堆表的第一页。

Table Meta的私有成员如下：

```c++
  table_id_t table_id_;
  std::string table_name_;
  page_id_t root_page_id_;
  Schema *schema_;
```

其中schema的序列化直接调用schema底层的序列化接口即可。

#### 1.3 Index Meta

Index Meta的序列化和Table Meta类似。但vector类型的key_map_和string类型的name都遵循序列化长度->序列化值的模式。

#### 1.4 Table Info

Table Info是表信息在内存中的存在形式，成员如下：

```c++
  TableMetadata *table_meta_;
  TableHeap *table_heap_;
  MemHeap *heap_; /** store all objects allocated in table_meta and table heap */
```

其中table_heap\_是使用table_meta\_中的first page id创建的堆表。





B+树的中间节点不存储数据，只存储键值和对应的子节点的逻辑页号，由于子节点的数量比键值多1，于是笔者采用了如下设计。

```c++
   KeyType key_[INTERNAL_PAGE_SIZE];
   page_id_t value_[INTERNAL_PAGE_SIZE + 1];
```

和1.2中原理相同，利用局部性原理。

### 2 B+ Tree Insert

***

B+树的插入作分类讨论如下：

+ 若B+树根节点为叶节点，直接插入根节点
+ 若B+树根节点为中间节点，则迭代找到对应插入键值的叶节点插入。
+ 若键值已经在叶中存在，结束操作

插入后的操作：

+ 若页节点大小小于MaxSize，结束操作
+ 否则对叶节点执行分裂操作，传出右边叶子的最小键值，插入父节点维护B+平衡。

插入父节点后的操作：

+ 若父节点大小小于MaxSize，结束操作
+ 否则对internal page执行分裂操作，额外将middle key分裂出，插入父节点，进行递归调用
+ 若该internal page是根节点，则执行分裂后额外分配新的根节点，连接。
+ 注意分裂时，要更新子节点的父节点值

### 3 B+ Tree Delete

***

B+树的删除操作步骤如下：

+ 首先找到删除键值对应的叶节点
+ 若键值不存在于节点中，结束操作，否则删除对应键值
+ 检查删除后的节点大小，若小于节点最小大小，则检查临近节点
  + 若该节点是父节点的最左节点，选取右侧sibling为临近节点
  + 否则选取左侧节点为临近节点
+ 找到临近节点后
  + 若临近节点大小大于MinSize, 从临近节点转移一个键值对到该节点，更新父节点中的键值
  + 若临近节点大小等于MinSize,将该节点和临近节点和父节点取出的对应键值合并成新的节点，递归检查父节点大小。
+ 若该节点为根节点大小为0, 则删除该节点。

#### 4 B+ Tree Search

***

B+树的等值查询通过迭代找到键值对应的叶子节点，线性扫描叶子节点得到值。

对于范围查询，对范围的端点创建迭代器，利用迭代器线性遍历进行范围查询。

### 5 B+ Tree Iterator

***

B+树迭代器成员如下：

```c++
  int index_;
  MappingType val_;
  B_PLUS_TREE_LEAF_PAGE_TYPE* leaf_page_;
  BufferPoolManager* buffer_pool_manager_;
```

index\_是该迭代器指向内容在叶节点中的下标。

val为std::pair<key,value>，是迭代器指向内容的只读。（B+索引的迭代器是只读的，这是因为B+树的键值的更改必须要通过B+树的插入和删除完成，因而B+树执行插入删除可能使得迭代器失效。）

### 6 B+ Tree Index

***

B+树索引是通过 B+ tree index这个类向外部暴露的，另外B+树在根节点改变时要通过存储与INDEX_ROOT_PAGE_ID页的IndexRootPage更改索引Id到索引根节点的映射。

### 7 Source Code

由于源码过大，见附录。



