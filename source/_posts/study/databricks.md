---
title: Databricks入门级学习 
tag: spark
categories: 技术栈  
---

直接上官方文档： [Azure Databricks 文档 | Microsoft Docs](https://docs.microsoft.com/zh-cn/azure/databricks/)

>  Databricks包含三大模块：
>
> 1. Databricks 数据科学与工程
>2. Databricks机器学习
> 3. Databricks SQL（预览版）
> 
> 这里我们目前学习的是Databricks数据科学与工程， 主要学习里边的Delta Lake 和 Delta Engine

> **Delta Lake 和 Delta Engine指南**
>
> **Delta Lake** 是可以提高 Data Lake 可靠性的开源存储层。 Delta Lake 提供 ACID 事务和可缩放的元数据处理，并可以统一流处理和批数据处理。 Delta Lake 在现有 Data Lake 的顶层运行，与 Apache Spark API 完全兼容。 利用 Azure Databricks 上的 Delta Lake，便可以根据工作负载模式配置 Delta Lake。
>
> Azure Databricks 还包括 **Delta Engine**，这为快速交互式查询提供了优化的布局和索引。



**写在前边**

1、因为这个技术是Azure新推出的，网上的推文或者解决问题的博客会比较少，这里自己记录一下项目开发过程中遇到的问题及解决方法，来当一个开拓者。

2、要想使用好这个API，得先利其器，那就把官网的文档先给过一遍。其中的新特性及API心里先有个大概印象，让我有机会去犯错，而不是不知道有这个功能。

3、通过读当前微软的Azure的文档，就是上边的官方文档，让我很清楚的知道了规整文字的力量，让我一步步的把这个技术看懂，看会。可能大部分的官方文档都是这样的，文档的目的就是为了能够让新手看懂。所以我们的个人笔记以及平时的工作汇报等各种方式的记录，都可以有理有据的有条不紊的进行记录和整理。

4、因为我目前用不到Python语言来进行这个项目的开发，所以这里的案例，我只用的SQL和Scala两种语言来进行记录此笔记。 



## 一、Delta Lake 快速入门 

### **1、创建表**

> 若要创建 Delta 表，可以使用现有的 Apache Spark SQL 代码，并将格式从 `parquet`、`csv`、`json` 等更改为 `delta`。
>
> 对于所有文件类型，都需要将文件读入数据帧并以 `delta` 格式写出

``` SQL 
SQL形式： 
CREATE TABLE events
USING delta
AS SELECT *
FROM json.`/data/events/`
---------------------------------------------------------------------------------------------------------------------
spark： 
events = spark.read.json("/databricks-datasets/structured-streaming/events/")
events.write.format("delta").save("/mnt/delta/events")
spark.sql("CREATE TABLE events USING DELTA LOCATION '/mnt/delta/events/'")
```

### 2、将数据分区

> 若要加速其谓词涉及分区列的查询，可以对数据进行分区。

``` SQL 
SQL形式：
若要在使用 SQL 创建 Delta 表时对数据进行分区，请指定 PARTITIONED BY 列。
CREATE TABLE events (
  date DATE,
  eventId STRING,
  eventType STRING,
  data STRING)
USING delta
PARTITIONED BY (date)
---------------------------------------------------------------------------------------------------------------------
Spark形式： 
events = spark.read.json("/databricks-datasets/structured-streaming/events/")
events.write.partitionBy("date").format("delta").save("/mnt/delta/events")
spark.sql("CREATE TABLE events USING DELTA LOCATION '/mnt/delta/events/'")
```

### 3、修改表

> Delta Lake 支持使用一组丰富的操作来修改表。

- **流式处理到表的写入**
你可以使用结构化流式处理将数据写入 Delta 表。 即使有针对表并行运行的其他流或批处理查询，Delta Lake 事务日志也可确保仅处理一次。 默认情况下，流在追加模式下运行，这会将新记录添加到表中。

```scala 
Python Spark ：

from pyspark.sql.types import *

inputPath = "/databricks-datasets/structured-streaming/events/"

jsonSchema = StructType([ StructField("time", TimestampType(), True), StructField("action", StringType(), True) ])

eventsDF = (
  spark
    .readStream
    .schema(jsonSchema) # Set the schema of the JSON data
    .option("maxFilesPerTrigger", 1) # Treat a sequence of files as a stream by picking one file at a time
    .json(inputPath)
)

(eventsDF.writeStream
  .outputMode("append")
  .option("checkpointLocation", "/mnt/delta/events/_checkpoints/etl-from-json")
  .table("events")
)
```

- **批量upsert**

若要将一组更新和插入合并到现有表中，请使用 `MERGE INTO` 语句。 例如，下面的语句将获取一个更新流，并将其合并到 `events` 表中。 如果已存在具有相同 `eventId` 的事件，Delta Lake 会使用给定的表达式更新数据列。 如果没有匹配的事件，Delta Lake 会添加一个新行。

```sql
MERGE INTO events
USING updates
ON events.eventId = updates.eventId
WHEN MATCHED THEN
  UPDATE SET
    events.data = updates.data
WHEN NOT MATCHED
  THEN INSERT (date, eventId, data) VALUES (date, eventId, data)
```

执行 `INSERT` 时必须为表中的每个列指定一个值（例如，当现有数据集中没有匹配行时，必须这样做）。 但是，你不需要更新所有值。

### 4、读取表

> 可以通过指定 DBFS 上的路径 (`"/mnt/delta/events"`) 或表名 (`"events"`) 来访问 Delta 表中的数据：

``` scala 
spark： 
val events = spark.read.format("delta").load("/mnt/delta/events")
或者
events = spark.table("events")
---------------------------------------------------------------------------------------------------------------------
SQL： 
SELECT * FROM delta.`/mnt/delta/events`
或
SELECT * FROM events
```

### 5、显示表历史记录

若要查看表的历史记录，请使用 `DESCRIBE HISTORY` 语句，该语句提供对表进行的每次写入的出处信息，包括表版本、操作、用户等。 请参阅[检索 Delta 表历史记录](https://docs.microsoft.com/zh-cn/azure/databricks/delta/delta-utility#delta-history)。

### 6、查询较早版本的表（按时间顺序查看）

Delta Lake 按时间顺序查看允许你查询 Delta 表的旧快照。

对于 `timestamp_string`，只接受日期或时间戳字符串。 例如，`"2019-01-01"` 和 `"2019-01-01'T'00:00:00.000Z"`。

若要查询较早版本的表，请在 `SELECT` 语句中指定版本或时间戳。 例如，若要从上述历史记录中查询版本 0，请使用：

``` sql
SELECT * FROM events VERSION AS OF 0
或 ---------------------------------------------------------------------------------------------------------------------
SELECT * FROM events TIMESTAMP AS OF '2019-01-29 00:37:58'

备注：
由于版本 1 位于时间戳 '2019-01-29 00:38:10' 处，因此，若要查询版本 0，可以使用范围 '2019-01-29 00:37:58' 到 '2019-01-29 00:38:09'（含）中的任何时间戳。
```

### 7、优化表

对表执行多个更改后，可能会有很多小文件。 为了提高读取查询的速度，你可以使用 `OPTIMIZE` 将小文件折叠为较大的文件：

``` sql 
OPTIMIZE delta.`/mnt/delta/events`
或
OPTIMIZE events
```

### 8、按列进行Z排序

为了进一步提高读取性能，你可以通过 Z 排序将相关的信息放置在同一组文件中。 Delta Lake 数据跳过算法会自动使用此并置，大幅减少需要读取的数据量。 若要对数据进行 Z 排序，请在 `ZORDER BY` 子句中指定要排序的列。 例如，若要按 `eventType` 并置，请运行：

```sql
OPTIMIZE events
  ZORDER BY (eventType)
```

有关运行 `OPTIMIZE` 时可用的完整选项集，请参阅[压缩（装箱）](https://docs.microsoft.com/zh-cn/azure/databricks/delta/optimizations/file-mgmt#delta-optimize)。

### 9、介绍性笔记本（Demo）

> 这里提供Scala 和 SQL的两个demo。 看了一遍之后，对上一章节的入门操作就有了大概的了解了。good，建议学习。
>
> 这些笔记本展示了如何将 JSON 数据转换为 Delta Lake 格式，创建 Delta 表，追加到表，优化生成的表，最后使用 Delta Lake 元数据命令显示表历史记录、格式和详细信息。
>
> 若要试用 Delta Lake，请参阅[快速入门：使用 Azure 门户在 Azure Databricks 上运行 Spark 作业](https://docs.microsoft.com/zh-cn/azure/azure-databricks/quickstart-create-databricks-workspace-portal)。

**Delta Lake 快速入门 Scala 笔记本** [获取笔记本](https://docs.microsoft.com/zh-cn/azure/databricks/_static/notebooks/delta/quickstart-scala.html)

**Delta Lake 快速入门 SQL 笔记本**  [获取笔记本](https://docs.microsoft.com/zh-cn/azure/databricks/_static/notebooks/delta/quickstart-sql.html)



## 二、数据的读取与写入

### 1、将数据引入Delta Lake

> Azure Databricks 提供多种方法来帮助你讲数据引入Delta Lake 
>
> - 合作伙伴集成
> - COPY INTO SQL 命令
> - 自动加载程序
>
> 上边这些，是直接提供的服务，如果能用到的话，再来看官网。 
>
>  [将数据引入到 Delta Lake - Azure Databricks - Workspace | Microsoft Docs](https://docs.microsoft.com/zh-cn/azure/databricks/delta/delta-ingest)

### 2、表批量读取和写入

> [表批量读取和写入 - Azure Databricks - Workspace | Microsoft Docs](https://docs.microsoft.com/zh-cn/azure/databricks/delta/delta-batch)
>
> 备注上官网文档路径。

#### 2.1、 创建表

> Delta Lake 支持创建两种类型的表：元存储中定义的表和由路径定义的表。

##### 2.1.1、创建表的三种方式。

- SQL DDL命令：可以使用 Apache Spark 支持的标准 SQL DDL 命令（例如 CREATE TABLE 和 REPLACE TABLE）来创建 Delta 表。

  ``` sql
  CREATE TABLE IF NOT EXISTS events (
    date DATE,
    eventId STRING,
    eventType STRING,
    data STRING)
  USING DELTA
  ---------------------------------------------------------------------------------------------------------------------
  CREATE OR REPLACE TABLE events (
    date DATE,
    eventId STRING,
    eventType STRING,
    data STRING)
  USING DELTA
  ```

- **`DataFrameWriter` API**：如果想要创建表，同时将 Spark DataFrame 或数据集中的数据插入到该表中，则可以使用 Spark `DataFrameWriter`

  ``` scala 
  // Create table in the metastore using DataFrame's schema and write data to it
  df.write.format("delta").saveAsTable("events")
  
  // Create table with path using DataFrame's schema and write data to it
  df.write.format("delta").mode("overwrite").save("/mnt/delta/events")
  ```

- **`DeltaTableBuilder` API**：也可以使用 Delta Lake 中的 `DeltaTableBuilder` API 创建表。 与 DataFrameWriter API 相比，此 API 可以更轻松地指定其他信息，例如列注释、表属性和[生成的列 ](https://docs.microsoft.com/zh-cn/azure/databricks/delta/delta-batch#deltausegeneratedcolumns)。

  ``` scala 
  // Create table in the metastore
  DeltaTable.createOrReplace(spark)
    .tableName("event")
    .addColumn("date", DateType)
    .addColumn("eventId", "STRING")
    .addColumn("eventType", StringType)
    .addColumn(
       DeltaTable.columnBuilder("data")
         .dataType("STRING")
         .comment("event data")
         .build())
    .execute()
  ---------------------------------------------------------------------------------------------------------------------
  // Create or replace table with path and add properties
  DeltaTable.createOrReplace(spark)
    .addColumn("date", DateType)
    .addColumn("eventId", "STRING")
    .addColumn("eventType", StringType)
    .addColumn(
       DeltaTable.columnBuilder("data")
         .dataType("STRING")
         .comment("event data")
         .build())
    .location("/mnt/delta/events")
    .property("description", "table with event data")
    .execute()
  ```

##### 2.1.2、创表时将数据分区

你可以对数据进行分区，以加速其谓词涉及分区列的查询或 DML。 若要在创建 Delta 表时对数据进行分区，请指定按列分区。 常见的模式是按日期进行分区，例如：

``` SQL
SQL ： 
-- Create table in the metastore
CREATE TABLE events (
  date DATE,
  eventId STRING,
  eventType STRING,
  data STRING)
USING DELTA
PARTITIONED BY (date)
---------------------------------------------------------------------------------------------------------------------
Scala：
df.write.format("delta").partitionBy("date").saveAsTable("events")
---------------------------------------------------------------------------------------------------------------------
DeltaTable.createOrReplace(spark)
  .tableName("event")
  .addColumn("date", DateType)
  .addColumn("eventId", "STRING")
  .addColumn("eventType", StringType)
  .addColumn("data", "STRING")
  .partitionedBy("date")
  .execute()
```

##### 2.1.3、空值数据位置

> 对于元存储中定义的表，可以选择性地将 `LOCATION` 指定为路径。 使用指定的 `LOCATION` 创建的表被视为不受元存储管理。 与不指定路径的托管表不同，非托管表的文件在你 `DROP` 表时不会被删除。
>
> 如果运行 `CREATE TABLE` 时指定的 `LOCATION` 已包含使用 Delta Lake 存储的数据，则 Delta Lake 会执行以下操作：
>
> - 如果仅指定了表名称和位置，例如：
>
>   SQL复制
>
>   ```sql
>   CREATE TABLE events
>   USING DELTA
>   LOCATION '/mnt/delta/events'
>   ```
>
>   元存储中的表会自动继承现有数据的架构、分区和表属性。 此功能可用于将数据“导入”到元存储中。
>
> - 如果你指定了任何配置（架构、分区或表属性），则 Delta Lake 会验证指定的内容是否与现有数据的配置完全匹配。
>
>    **重要**：如果指定的配置与数据的配置并非完全匹配，则 Delta Lake 会引发一个描述差异的异常。

#### 2.2、读取表

> 可以通过指定一个表名或路径将 Delta 表作为数据帧加载：

``` sql 
SQL:
SELECT * FROM events   -- query table in the metastore
SELECT * FROM delta.`/mnt/delta/events`  -- query table by path  
---------------------------------------------------------------------------------------------------------------------
Scala:
spark.table("events")      // query table in the metastore
spark.read.format("delta").load("/mnt/delta/events")  // create table by path
import io.delta.implicits._
spark.read.delta("/mnt/delta/events")
```

数据保留

> 若要按时间顺序查看以前的某个版本，必须同时保留该版本的日志文件和数据文件。
>
> 永远不会自动删除支持 Delta 表的数据文件；仅在运行 [VACUUM](https://docs.microsoft.com/zh-cn/azure/databricks/delta/delta-utility#delta-vacuum) 时才会删除数据文件。 `VACUUM` 不会删除 Delta 日志文件；在写入检查点后自动清理日志文件。

#### 2.3、写入到表

- 追加:要将新数据以原子方式添加到现有 Delta 表，请使用 `append` 模式：

  ```SQL 
  SQL: 
  INSERT INTO events SELECT * FROM newEvents
  ---------------------------------------------------------------------------------------------------------------------
  scala :
  df.write.format("delta").mode("append").save("/mnt/delta/events")
  df.write.format("delta").mode("append").saveAsTable("events")
  ---------------------------------------------------------------------------------------------------------------------
  import io.delta.implicits._
  df.write.mode("append").delta("/mnt/delta/events")
  ```

- Overwrite : 要以原子方式替换表中的所有数据，请使用 `overwrite` 模式：

  ``` SQL 
  SQL: 
  INSERT OVERWRITE TABLE events SELECT * FROM newEvents
  ---------------------------------------------------------------------------------------------------------------------
  Scala:
  df.write.format("delta").mode("overwrite").save("/mnt/delta/events")
  df.write.format("delta").mode("overwrite").saveAsTable("events")
  
  import io.delta.implicits._
  df.write.mode("overwrite").delta("/mnt/delta/events")
  ```

#### 2.4、更新表结构，替换表结构

- Delta Lake 允许你更新表的架构。 支持下列类型的更改：

  - 添加新列（在任意位置）
  - 重新排列现有列

  你可以使用 DDL 显式地或使用 DML 隐式地进行这些更改。

- 默认情况下，覆盖表中的数据不会覆盖架构。 在不使用 `replaceWhere` 的情况下使用 `mode("overwrite")` 来覆盖表时，你可能还希望覆盖写入的数据的架构。 你可以通过将 `overwriteSchema` 选项设置为 `true` 来替换表的架构和分区：

  ```python
  df.write.option("overwriteSchema", "true")
  ```

#### 2.5、表中的视图，表属性，表元数据

- 表中的视图：Delta Lake 支持基于 Delta 表创建视图，就像使用数据源表一样。

  这些视图集成了[表访问控制](https://docs.microsoft.com/zh-cn/azure/databricks/security/access-control/table-acls/object-privileges)，可以实现列级和行级安全性。

  处理视图时的主要难题是解析架构。 如果你更改 Delta 表架构，则必须重新创建派生视图，以容纳向该架构添加的任何内容。 例如，如果向 Delta 表中添加一个新列，则必须确保此列在基于该基表构建的相应视图中可用。

- 表属性：你可以使用 `CREATE` 和 `ALTER` 中的 `TBLPROPERTIES` 将自己的元数据存储为表属性。

  `TBLPROPERTIES` 存储为 Delta 表元数据的一部分。 如果在给定位置已存在 Delta 表，则无法在 `CREATE` 语句中定义新的 `TBLPROPERTIES`。 有关更多详细信息，请参阅[创建表](https://docs.microsoft.com/zh-cn/azure/databricks/delta/delta-batch#ddlcreatetable)。

  此外，为了调整行为和性能，Delta Lake 支持某些 Delta 表属性：

  - 阻止 Delta 表中的删除和更新：`delta.appendOnly=true`。
  - 配置[按时间顺序查看](https://docs.microsoft.com/zh-cn/azure/databricks/delta/delta-batch#deltatimetravel)保留属性：`delta.logRetentionDuration=<interval-string>` 和 `delta.deletedFileRetentionDuration=<interval-string>`。 有关详细信息，请参阅[数据保留](https://docs.microsoft.com/zh-cn/azure/databricks/delta/delta-batch#data-retention)。
  - 配置要为其收集统计信息的列的数目：`delta.dataSkippingNumIndexedCols=<number-of-columns>`。 此属性仅对写出的新数据有效。

   备注

  - 修改 Delta 表属性是一个写入操作，该操作会与其他[并发写入操作](https://docs.microsoft.com/zh-cn/azure/databricks/delta/concurrency-control)冲突，导致这些操作失败。 建议仅当不存在对表的并发写入操作时才修改表属性。

  你还可以在第一次提交到 Delta 表期间使用 Spark 配置来设置带 `delta.` 前缀的属性。 例如，若要使用属性 `delta.appendOnly=true` 初始化 Delta 表，请将 Spark 配置 `spark.databricks.delta.properties.defaults.appendOnly` 设置为 `true`。 例如： 。

  ```sql
  SQL：
  spark.sql("SET spark.databricks.delta.properties.defaults.appendOnly = true")
  
  Scala：
  spark.conf.set("spark.databricks.delta.properties.defaults.appendOnly", "true")
  ```

- 表元数据：Delta Lake 提供了丰富的用来浏览表元数据的功能。
  - DESCRIBE DETAIL：提供架构、分区、表大小等方面的信息。
  - DESCRIBE HISTORY：提供出处信息，包括操作、用户等，以及向表的每次写入的操作指标。 表历史记录会保留 30 天。

#### 2.6、Delta Lake 批处理命令笔记本（Demo）

[获取笔记本](https://docs.microsoft.com/zh-cn/azure/databricks/_static/notebooks/delta/quickstart-sql.html)

### 3、表流读取和写入

> Delta Lake 通过 `readStream` 和 `writeStream` 与 [Spark 结构化流式处理](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)深度集成。 Delta Lake 克服了通常与流式处理系统和文件相关的许多限制，包括：
>
> - 合并低延迟引入生成的小文件
> - 保持对多个流（或并发批处理作业）执行“仅一次”处理
> - 使用文件作为流源时，可以有效地发现哪些文件是新文件

#### 3.1 、用作源的 Delta 表

> 将 Delta 表作为流源加载并在流式处理查询中使用它时，该查询将处理表中存在的所有数据以及流启动后到达的所有新数据。
>
> 可以将路径和表都作为流加载。

```scala
spark.readStream.format("delta")
  .load("/mnt/delta/events")

import io.delta.implicits._
spark.readStream.delta("/mnt/delta/events")

或：--------------------------------------------------------------------------------------------------------------------- 

import io.delta.implicits._

spark.readStream.format("delta").table("events")
```

##### 3.1.1、限制输入速率

> 以下选项可用于控制微批处理：
>
> - `maxFilesPerTrigger`：每个微批处理中要考虑的新文件数。 默认值为 1000。
> - `maxBytesPerTrigger`：每个微批处理中处理的数据量。 该选项会设置“软最大值”，即批处理大约可以处理这一数量的数据，并且可能处理超出该限制的数据量。 如果将 `Trigger.Once` 用于流式处理，此选项将被忽略。 默认情况下，未设置此项。
>
> 如果将 `maxBytesPerTrigger` 与 `maxFilesPerTrigger` 结合使用，则微批处理将处理数据，直到达到 `maxFilesPerTrigger` 或 `maxBytesPerTrigger` 限制。
>
>  备注: 
>
> 如果源表事务由于 `logRetentionDuration` [配置](https://docs.microsoft.com/zh-cn/azure/databricks/delta/delta-batch#data-retention)而被清除，并且流在处理中滞后，则 Delta Lake 会处理与源表的最新可用事务历史记录相对应的数据，但不会使流失败。 这可能会导致数据被丢弃。

##### 3.1.2、忽略更新和删除

> 结构化流式处理不处理非追加的输入，并且会在对用作源的表进行了任何修改时引发异常。 可以通过两种主要策略处理无法自动向下游传播的更改：
>
> - 可以删除输出和检查点，并从头开始重启流。
> - 可以设置以下两个选项之一：
>   - `ignoreDeletes`：忽略在分区边界删除数据的事务。
>   - `ignoreChanges`：如果由于数据更改操作（例如 `UPDATE`、`MERGE INTO`、分区内的 `DELETE` 或 `OVERWRITE`）而不得不在源表中重写文件，则重新处理更新。 未更改的行仍可能发出，因此下游使用者应该能够处理重复项。 删除不会传播到下游。 `ignoreChanges` 包括 `ignoreDeletes`。 因此，如果使用 `ignoreChanges`，则流不会因源表的删除或更新而中断。

**示例：**

例如，假设你有一个表 `user_events`，其中包含 `date`、`user_email` 和 `action` 列，并按 `date` 对该表进行了分区。 从 `user_events` 表向外进行流式处理，由于 GDPR 的原因，需要从中删除数据。

在分区边界（即 `WHERE` 位于分区列上）执行删除操作时，文件已经按值进行了分段，因此删除操作直接从元数据中删除这些文件。 因此，如果只想删除某些分区中的数据，则可以使用：

```scala
spark.readStream.format("delta")
  .option("ignoreDeletes", "true")
  .load("/mnt/delta/user_events")
```

但是，如果必须基于 `user_email` 删除数据，则需要使用：

```scala
spark.readStream.format("delta")
  .option("ignoreChanges", "true")
  .load("/mnt/delta/user_events")
```

如果使用 `UPDATE` 语句更新 `user_email`，则包含相关 `user_email` 的文件将被重写。 使用 `ignoreChanges` 时，新记录将与同一文件中的所有其他未更改记录一起传播到下游。 逻辑应该能够处理这些传入的重复记录。

##### 3.1.3、指定初始位置

> 此功能在 Databricks Runtime 7.3 LTS 及更高版本上可用。
>
> 可以使用以下选项来指定 Delta Lake 流式处理源的起点，而无需处理整个表。
>
> - `startingVersion`：要从其开始的 Delta Lake 版本。 从此版本（含）开始的所有表更改都将由流式处理源读取。 可以从 [DESCRIBE HISTORY](https://docs.microsoft.com/zh-cn/azure/databricks/delta/delta-utility#delta-history) 命令输出的 `version` 列中获取提交版本。
>
>   若要仅返回 Databricks Runtime 7.4 及更高版本中的最新更改，请指定 `latest`。
>
> - `startingTimestamp`：要从其开始的时间戳。 在该时间戳（含）或之后提交的所有表更改都将由流式处理源读取。 下列其中一项：
>
>   - 时间戳字符串。 例如 `"2019-01-01T00:00:00.000Z"`。
>   - 日期字符串。 例如 `"2019-01-01"`。
>
> 不能同时设置这两个选项，只能使用其中一个选项。 这两个选项仅在启动新的流式处理查询时才生效。 如果流式处理查询已启动且已在其检查点中记录进度，这些选项将被忽略。
>
>  重要:虽然可以从指定的版本或时间戳启动流式处理源，但流式处理源的架构始终是 Delta 表的最新架构。 必须确保在指定版本或时间戳之后，不对 Delta 表进行任何不兼容的架构更改。 否则，使用错误的架构读取数据时，流式处理源可能会返回不正确的结果。

示例：

``` scala 
例如，假设你有一个表 user_events。 如果要从版本 5 开始读取更改，请使用：
spark.readStream.format("delta")
  .option("startingVersion", "5")
  .load("/mnt/delta/user_events")

如果想了解自 2018 年 10 月 18 日以来进行的更改，可使用：
spark.readStream.format("delta")
  .option("startingTimestamp", "2018-10-18")
  .load("/mnt/delta/user_events")
```

#### 3.2、用作接收器的Delta表

你也可以使用结构化流式处理将数据写入 Delta 表。 即使有针对表并行运行的其他流或批处理查询，Delta Lake 也可通过事务日志确保“仅一次”处理。

##### 3.2.1、追加模式

> 默认情况下，流在追加模式下运行，这会将新记录添加到表中。

- 可以使用路径方法：	

  ``` scala 
  events.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/mnt/delta/events/_checkpoints/etl-from-json")
    .start("/mnt/delta/events")
  
  import io.delta.implicits._
  events.writeStream
    .outputMode("append")
    .option("checkpointLocation", "/mnt/delta/events/_checkpoints/etl-from-json")
    .delta("/mnt/delta/events")
  ```

- 或表方法：

  ``` scala 
  events.writeStream
    .outputMode("append")
    .option("checkpointLocation", "/mnt/delta/events/_checkpoints/etl-from-json")
    .table("events")
  ```

##### 3.2.2、完整模式

> 你还可以使用结构化流式处理将整个表替换为每个批。 一个示例用例是使用聚合来计算摘要：

```scala
spark.readStream
  .format("delta")
  .load("/mnt/delta/events")
  .groupBy("customerId")
  .count()
  .writeStream
  .format("delta")
  .outputMode("complete")
  .option("checkpointLocation", "/mnt/delta/eventsByCustomer/_checkpoints/streaming-agg")
  .start("/mnt/delta/eventsByCustomer")
```

上述示例持续更新包含按客户划分的事件总数的表。

对于延迟要求较为宽松的应用程序，可以使用一次性触发器来节省计算资源。 使用这些触发器按给定计划更新汇总聚合表，从而仅处理自上次更新以来收到的新数据。

##### 3.2.3、幂等多表写入

> 备注:适用于 Databricks Runtime 8.4 及更高版本。
>
> 本部分介绍如何使用 [foreachBatch](https://docs.microsoft.com/zh-cn/azure/databricks/spark/latest/structured-streaming/foreach) 命令写入多个表。 通过`foreachBatch` 可将流查询中每个微批处理的输出写入多个目标。 但是，`foreachBatch` 不会使这些写入具有幂等性，因为这些写入尝试缺少批处理是否正在重新执行的信息。 例如，重新运行失败的批处理可能会导致重复的数据写入。
>
> 为了解决这个问题，Delta 表支持以下 DataFrameWriter 选项以使写入具有幂等性：
>
> - `txnAppId`：可以在每次 `DataFrame` 写入时传递的唯一字符串。 例如，可以使用 StreamingQuery ID 作为 `txnAppId`。
> - `txnVersion`：充当事务版本的单调递增数字。
>
> Delta Lake 使用 `txnAppId` 和 `txnVersion` 的组合来识别重复写入并忽略它们。
>
> 如果批量写入因失败而中断，则重新运行该批次将使用相同的应用程序和批次 ID，这将有助于运行时正确识别重复写入并忽略它们。 应用程序 ID (`txnAppId`) 可以是任何用户生成的唯一字符串，不必与流 ID 相关。
>
>  **警告**:如果删除流式处理检查点并重启查询，则必须提供一个不同的 `appId`；否则，来自重启查询的写入将被忽略，因为它包含相同的 `txnAppId`，并且批处理 ID 从 0 开始。
>
> 举例： 

``` scala 
appId = ... // A unique string that is used as application ID.
streamingDF.writeStream.foreachBatch { (batchDF: DataFrame, batchId: Long) =>
  batchDF.write.format(...).option("txnVersion", batchId).option("txnAppId", appId).save(...)  // location 1
  batchDF.write.format(...).option("txnVersion", batchId).option("txnAppId", appId).save(...)  // location 2
}
```

### 4、表删除、更新和合并

> Delta Lake 支持多个语句，以便在 Delta 表中删除数据和更新数据。

#### 4.1、从表中删除

> 可以从 Delta 表中删除与谓词匹配的数据。 例如，若要删除 `2017` 之前的所有事件，可以运行以下命令：

```sql
SQL : 
DELETE FROM events WHERE date < '2017-01-01'
DELETE FROM delta.`/data/events/` WHERE date < '2017-01-01'

---------------------------------------------------------------------------------------------------------------------
Scala:可在 Databricks Runtime 6.0 及更高版本中使用 Scala API。

import io.delta.tables._
val deltaTable = DeltaTable.forPath(spark, "/data/events/")
deltaTable.delete("date < '2017-01-01'")        // predicate using SQL formatted string

import org.apache.spark.sql.functions._
import spark.implicits._
deltaTable.delete(col("date") < "2017-01-01")       // predicate using Spark SQL functions and implicits
```

重要： `delete` 可以从最新版本的 Delta 表中删除数据，但是直到显式删除旧的版本后才能从物理存储中删除数据。

提示：如果可能，请在分区的 Delta 表的分区列上提供谓词，因为这样的谓词可以显著加快操作速度。

#### 4.2、更新表

> 可以在 Delta 表中更新与谓词匹配的数据。 
>
> 例如，若要解决 `eventType` 中的拼写错误，可以运行以下命令：

``` sql 
SQL:
UPDATE events SET eventType = 'click' WHERE eventType = 'clck'
UPDATE delta.`/data/events/` SET eventType = 'click' WHERE eventType = 'clck'

---------------------------------------------------------------------------------------------------------------------
Scala:可在 Databricks Runtime 6.0 及更高版本中使用 Scala API。
import io.delta.tables._
val deltaTable = DeltaTable.forPath(spark, "/data/events/")
deltaTable.updateExpr(            // predicate and update expressions using SQL formatted string
  "eventType = 'clck'",
  Map("eventType" -> "'click'")

import org.apache.spark.sql.functions._
import spark.implicits._
deltaTable.update(                // predicate using Spark SQL functions and implicits
  col("eventType") === "clck",
  Map("eventType" -> lit("click")));

```

提示： 与删除类似，在分区上使用谓词可以显著提高更新操作的速度。



#### 4.3、使用合并操作在表中执行更新插入

> 可以使用 `MERGE` SQL 操作将源表、视图或 DataFrame 中的数据更新插入到 Delta 表中。 Delta Lake 支持 `MERGE` 中的插入、更新和删除，并支持超出 SQL 标准的扩展语法以辅助高级用例。
>
> 假设你有一个 Spark DataFrame，它包含带有 `eventId` 的事件的新数据。 其中一些事件可能已经存在于 `events` 表中。 若要将新数据合并到 `events` 表中，你需要更新匹配的行（即，`eventId` 已经存在）并插入新行（即，`eventId` 不存在）。 可以运行以下查询：

``` SQL 
SQL:

MERGE INTO events
USING updates
ON events.eventId = updates.eventId
WHEN MATCHED THEN
  UPDATE SET events.data = updates.data
WHEN NOT MATCHED THEN 
  INSERT (date, eventId, data) VALUES (date, eventId, data)
---------------------------------------------------------------------------------------------------------------------
Scala形式: 

import io.delta.tables._
import org.apache.spark.sql.functions._

val updatesDF = ...  // define the updates DataFrame[date, eventId, data]

DeltaTable.forPath(spark, "/data/events/")
  .as("events")
  .merge(
    updatesDF.as("updates"),
    "events.eventId = updates.eventId")
  .whenMatched   -- 当匹配上，执行update
  .updateExpr(
    Map("data" -> "updates.data"))
  .whenNotMatched  -- 当没匹配上，执行insert
  .insertExpr(
    Map(
      "date" -> "updates.date",
      "eventId" -> "updates.eventId",
      "data" -> "updates.data"))
  .execute()
```

**操作语义：**

下面是 `merge` 编程操作的详细说明。

- 可以有任意数量的 `whenMatched` 和 `whenNotMatched` 子句。

   备注

  在 Databricks Runtime 7.2 及更低版本中，`merge` 最多可以有 2 个 `whenMatched` 子句和最多 1 个 `whenNotMatched` 子句。

- 当源行根据匹配条件与目标表行匹配时，将执行 `whenMatched` 子句。 这些子句具有以下语义。

  - `whenMatched` 子句最多可以有 1 个 `update` 和 1 个 `delete` 操作。 `merge` 中的 `update` 操作只更新匹配目标行的指定列（类似于 `update` [操作](https://docs.microsoft.com/zh-cn/azure/databricks/delta/delta-update#delta-update)）。 `delete` 操作删除匹配的行。

  - 每个 `whenMatched` 子句都可以有一个可选条件。 如果存在此子句条件，则仅当该子句条件成立时，才对任何匹配的源-目标行对执行 `update` 或 `delete` 操作。

  - 如果有多个 `whenMatched` 子句，则将按照指定的顺序对其进行求值（即，子句的顺序很重要）。 除最后一个之外，所有 `whenMatched` 子句都必须具有条件。

  - 如果多个 `whenMatched` 子句都具有条件，并且对于匹配的源-目标行对都没有条件成立，那么匹配的目标行将保持不变。

  - 若要使用源数据集的相应列更新目标 Delta 表的所有列，请使用 `whenMatched(...).updateAll()`。 这等效于：

    ```scala
    whenMatched(...).updateExpr(Map("col1" -> "source.col1", "col2" -> "source.col2", ...))
    ```

    针对目标 Delta 表的所有列。 因此，此操作假定源表的列与目标表的列相同，否则查询将引发分析错误。

     备注

    启用自动架构迁移后，此行为将更改。 有关详细信息，请参阅[自动架构演变](https://docs.microsoft.com/zh-cn/azure/databricks/delta/delta-update#merge-schema-evolution)。

- 当源行根据匹配条件与任何目标行都不匹配时，将执行 `whenNotMatched` 子句。 这些子句具有以下语义。

  - `whenNotMatched` 子句只能具有 `insert` 操作。 新行是基于指定的列和相应的表达式生成的。 你无需指定目标表中的所有列。 对于未指定的目标列，将插入 `NULL`。

     备注

    在 Databricks Runtime 6.5 及更低版本中，必须为 `INSERT` 操作提供目标表中的所有列。

  - 每个 `whenNotMatched` 子句都可以有一个可选条件。 如果存在子句条件，则仅当源条件对该行成立时才插入该行。 否则，将忽略源列。

  - 如果有多个 `whenNotMatched` 子句，则将按照指定的顺序对其进行求值（即，子句的顺序很重要）。 除最后一个之外，所有 `whenNotMatched` 子句都必须具有条件。

  - 若要使用源数据集的相应列插入目标 Delta 表的所有列，请使用 `whenNotMatched(...).insertAll()`。 这等效于：

    ```scala
    whenNotMatched(...).insertExpr(Map("col1" -> "source.col1", "col2" -> "source.col2", ...))
    ```

    针对目标 Delta 表的所有列。 因此，此操作假定源表的列与目标表的列相同，否则查询将引发分析错误。

     备注:启用自动架构迁移后，此行为将更改。 有关详细信息，请参阅[自动架构演变](https://docs.microsoft.com/zh-cn/azure/databricks/delta/delta-update#merge-schema-evolution)。

 **重要**

- 如果源数据集的多行匹配，并且合并尝试更新目标 Delta 表的相同行，则 `merge` 操作可能会失败。 根据合并的 SQL 语义，这种更新操作模棱两可，因为尚不清楚应使用哪个源行来更新匹配的目标行。 你可以预处理源表来消除出现多个匹配项的可能性。 请参阅[变更数据捕获示例](https://docs.microsoft.com/zh-cn/azure/databricks/delta/delta-update#write-change-data-into-a-delta-table) - 它显示如何对变更数据集（即源数据集）进行预处理，以仅保留每键的最新更改，然后再将更改应用到目标 Delta 表中。
- 如果源数据集是非确定性的，则merge操作可能会产生不正确的结果。 这是因为merge可能会对源数据集执行两次扫描，如果两次扫描产生的数据不同，则对表所做的最终更改可能不正确。 源中的非确定性可能以多种方式出现。 其中一些如下：
  - 从非 Delta 表读取。 例如，从 CSV 表中读取，其中多次扫描时的基础文件可能会有所不同。
  - 使用非确定性操作。 例如，使用当前时间戳筛选数据的 `Dataset.filter()` 操作可以在多次扫描之间产生不同的结果。
- 仅当视图已定义为 `CREATE VIEW viewName AS SELECT * FROM deltaTable` 时，才能对 SQL VIEW 应用 SQL `MERGE` 操作。

 备注: 在 Databricks Runtime 7.3 LTS 及更高版本中，无条件删除匹配项时允许多个匹配项（因为即使有多个匹配项，无条件删除也非常明确）。



##### 4.3.1、**性能调优**

可以使用以下方法缩短合并所用时间：

- **减少匹配项的搜索空间**：默认情况下，操作将搜索整个 `merge` Delta 表以查找源表中的匹配项。 加速 `merge` 的一种方法是通过在匹配条件中添加已知约束来缩小搜索范围。 例如，假设你有一个由 `country` 和 `date` 分区的表，并且你希望使用 `merge` 更新最后一天和特定国家/地区的信息。 添加条件

  ```sql
  events.date = current_date() AND events.country = 'USA'
  ```

  将使查询更快，因为它只在相关分区中查找匹配项。 此外，该方法还有助于减少与其他并发操作发生冲突的机会。 有关详细信息，请参阅[并发控制](https://docs.microsoft.com/zh-cn/azure/databricks/delta/concurrency-control)。

- **压缩文件**：如果数据存储在许多小文件中，则读取数据以搜索匹配项可能会变慢。 可以将小文件压缩为更大的文件，以提高读取吞吐量。 有关详细信息，请参阅[压缩文件](https://docs.microsoft.com/zh-cn/azure/databricks/delta/best-practices#compact-files)。

- **控制写入的随机** 分区：操作会多次混用数据， `merge` 以计算和写入更新的数据。 用于随机排列的任务的数量由 Spark 会话配置 `spark.sql.shuffle.partitions` 控制。 设置此参数不仅可以控制并行度，还可以确定输出文件的数量。 增大该值可提高并行度，但也会生成大量较小的数据文件。

- **启用优化写入**：对于已分区表，`merge` 生成的小文件数量远大于随机分区的数量。 这是因为每个随机任务都可以在多个分区中写入多个文件，并可能成为性能瓶颈。 可以通过启用[优化写入](https://docs.microsoft.com/zh-cn/azure/databricks/delta/optimizations/auto-optimize#how-auto-optimize-works)来减少文件数量。

 备注

在 Databricks Runtime 7.4 及更高版本中，在对分区表进行 `merge` 操作时，会自动启用[优化写入](https://docs.microsoft.com/zh-cn/azure/databricks/delta/optimizations/auto-optimize#how-auto-optimize-works)。

- **调整表中的文件大小**：自 <DBR 8.2> 起，Azure Databricks 可以自动检测增量表是否在频繁执行重写文件的 `merge` 操作，并可能会减小重写文件的大小，以备将来执行更多文件重写操作。 有关详细信息，请参阅有关[调整文件大小](https://docs.microsoft.com/zh-cn/azure/databricks/delta/optimizations/file-mgmt#autotune-based-on-table-size)的部分。

#### 4.4、合并操作示例

##### 4.4.1、写入 Delta 表时进行重复数据删除

> 一个常见的 ETL 用例是通过将日志附加到表中来将其收集到 Delta 表中。 但是，源通常可以生成重复的日志记录，因此需要下游重复数据删除步骤来处理它们。 通过 `merge`，你可以避免插入重复记录。
>
> ```SQL 
> SQL :
> MERGE INTO logs
> USING newDedupedLogs
> ON logs.uniqueId = newDedupedLogs.uniqueId
> WHEN NOT MATCHED
>   THEN INSERT *
>   
> Scala： -------------------------------------------------------------------------------------- 
>   deltaTable
>   .as("logs")
>   .merge(
>     newDedupedLogs.as("newDedupedLogs"),
>     "logs.uniqueId = newDedupedLogs.uniqueId")
>   .whenNotMatched()
>   .insertAll()
>   .execute()
> ```

> 如果你知道几天之内可能会得到重复记录，则可以通过按日期对表进行分区，然后指定要匹配的目标表的日期范围来进一步优化查询。
>
> ```SQL
> SQL:
> MERGE INTO logs
> USING newDedupedLogs
> ON logs.uniqueId = newDedupedLogs.uniqueId AND logs.date > current_date() - INTERVAL 7 DAYS
> WHEN NOT MATCHED AND newDedupedLogs.date > current_date() - INTERVAL 7 DAYS
>   THEN INSERT *
>   
> Scala： -------------------------------------------------------------------------------------- 
> deltaTable.as("logs").merge(
>     newDedupedLogs.as("newDedupedLogs"),
>     "logs.uniqueId = newDedupedLogs.uniqueId AND logs.date > current_date() - INTERVAL 7 DAYS")
>   .whenNotMatched("newDedupedLogs.date > current_date() - INTERVAL 7 DAYS")
>   .insertAll()
>   .execute()
> ```

##### 4.4.2、使用 从流式处理查询更新插入foreachBatch

> 可以使用 `merge` 和 `foreachBatch` 的组合（有关详细信息，请参阅 [foreachbatch](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#foreachbatch)）将复杂的更新插入操作从流式处理查询写入 Delta 表。 例如： 。
>
> - **在更新模式下写入流式处理聚合**：这比完整模式高效得多。
> - **将数据库更改流写入 Delta** 表：用于写入 更改数据的合并查询可用于持续将更改 `foreachBatch` 流应用于 Delta 表。
> - 使用重复数据删除将流数据写入 **Delta** 表：在 中，重复数据删除的仅插入合并查询可用于将数据 (和重复) 写入到具有自动重复数据删除的 `foreachBatch` Delta 表。
>
>  备注
>
> - 请确保 `foreachBatch` 中的 `merge` 语句是幂等的，因为重启流式处理查询可以将操作多次应用于同一批数据。
> - 在 `foreachBatch` 中使用 `merge` 时，流式处理查询的输入数据速率（通过 `StreamingQueryProgress` 报告并在笔记本计算机速率图中可见）可以报告为源处生成数据的实际速率的倍数。 这是因为 `merge` 多次读取输入数据，导致输入指标倍增。 如果这是一个瓶颈，则可以在 `merge` 之前缓存批处理 DataFrame，然后在 `merge` 之后取消缓存。

使用 merge 和 foreachBatch 笔记本在更新模式下写入流式处理聚合(demo) [获取笔记本](https://docs.microsoft.com/zh-cn/azure/databricks/_static/notebooks/merge-in-streaming.html)

### 5、更改数据馈送

> 备注：可在 Databricks Runtime 8.4 及更高版本中使用。
>
> 增量更改数据馈送表示 Delta 表的不同版本之间的行级别更改。 对 Delta 表启用此功能后，运行时会记录写入该表的所有数据的“更改事件”。 这包括行数据以及指示已插入、已删除还是已更新指定行的元数据。
>
> 可在批处理查询中使用 SQL 和 DataFrame API（即 `df.read`）读取更改事件，并且可在流式处理查询中使用 DataFrame API（即 `df.readStream`）读取更改事件。

#### 5.1、用例

> 默认情况下，不启用增量更改数据馈送。 启用更改数据馈送有助于实现以下用例。
>
> - **银色和金色表**：仅通过处理初始执行 `MERGE`、`UPDATE` 或 `DELETE` 操作后发生的行级别更改来加速和简化 ETL 和 ELT 操作，从而提高 Delta 性能。
> - **具体化视图**：创建最新的聚合信息视图，以在 BI 和分析中使用，无需重新处理整个基础表，而是仅在发生更改时更新。
> - **传输更改**：将更改数据馈送发送至下游系统（如 Kafka 或 RDBMS），这些系统可使用它在数据管道后期阶段以增量方式进行处理。
> - 审核线索表：捕获更改数据馈送作为 Delta 表可提供永久存储和高效的查询功能，用于查看一段时间的所有更改，包括何时发生删除和进行了哪些更新。

### 5.2、启用更改数据馈送

- **重要提示**

> - 为表启用更改数据馈送选项后，无法再使用 Databricks Runtime 8.1 或更低版本写入该表。 始终可以读取该表。
> - 仅记录启用更改数据馈送后所做的更改；不会捕获之前对表所做的更改。
>
> 必须使用以下方法显式启用更改数据馈送选项

**新表**：在 `CREATE TABLE` 命令中设置表属性 `delta.enableChangeDataFeed = true`。

```sql
CREATE TABLE student (id INT, name STRING, age INT) TBLPROPERTIES (delta.enableChangeDataFeed = true)
```

**现有表**：在 `ALTER TABLE` 命令中设置表属性 `delta.enableChangeDataFeed = true`。

```sql
ALTER TABLE myDeltaTable SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
```

**所有新表**：

```sql
set spark.databricks.delta.properties.defaults.enableChangeDataFeed = true;
```

- 更改数据存储位置：

> Azure Databricks 将 `UPDATE`、`DELETE` 和 `MERGE` 操作的更改数据记录在 Delta 表目录的 `_change_data` 文件夹下。 当 Azure Databricks 检测到它可以直接从事务日志计算更改数据馈送时，可能会跳过这些记录。 具体而言，仅插入操作和完整分区删除操作不会在 `_change_data` 目录中生成数据。
>
> `_change_data` 文件夹中的文件遵循表的保留策略。 因此，如果运行 [VACUUM](https://docs.microsoft.com/zh-cn/azure/databricks/delta/delta-utility#delta-vacuum) 命令，也会删除更改数据馈送数据。

### 5.3、在批处理查询中读取更改

> 可在开始和结束时提供版本或时间戳。 开始和结束版本以及时间戳包含在查询中。 若要读取从表的特定开始版本到最新版本的更改，仅指定起始版本或时间戳。
>
> 将版本指定为整数，将时间戳指定为字符串，格式为 `yyyyMMddHHmmssSSS`。
>
> 如果提供的版本较低或提供的时间戳早于已记录更改事件的时间戳，那么启用更改数据馈送时，会引发错误，指示未启用更改数据馈送。

``` SQL 
SQL: 
-- version as ints or longs e.g. changes from version 0 to 10
SELECT * FROM table_changes('tableName', 0, 10)

-- timestamp as string formatted timestamps
SELECT * FROM table_changes('tableName', '2021-04-21 05:45:46', '2021-05-21 12:00:00')

-- providing only the startingVersion/timestamp
SELECT * FROM table_changes('tableName', 0)

-- database/schema names inside the string for table name, with backticks for escaping dots and special characters
SELECT * FROM table_changes('dbName.`dotted.tableName`', '2021-04-21 06:45:46' , '2021-05-21 12:00:00')

-- path based tables
SELECT * FROM table_changes_by_path('\path', '2021-04-21 05:45:46')

--------------------------------------------------------------------------------------------------------------
Spark:
// version as ints or longs
spark.read.format("delta")
  .option("readChangeFeed", "true")
  .option("startingVersion", 0)
  .option("endingVersion", 10)
  .table("myDeltaTable")

// timestamps as formatted timestamp
spark.read.format("delta")
  .option("readChangeFeed", "true")
  .option("startingTimestamp", "2021-04-21 05:45:46")
  .option("endingTimestamp", "2021-05-21 12:00:00")
  .table("myDeltaTable")

// providing only the startingVersion/timestamp
spark.read.format("delta")
  .option("readChangeFeed", "true")
  .option("startingVersion", 0)
  .table("myDeltaTable")

// path based tables
spark.read.format("delta")
  .option("readChangeFeed", "true")
  .option("startingTimestamp", "2021-04-21 05:45:46")
  .load("pathToMyDeltaTable")
```

### 5.4、在流式处理查询中读取更改

> 若要在读取表的同时获取更改数据，请将选项 `readChangeFeed` 设置为 `true`。 `startingVersion` 或 `startingTimestamp` 是可选项，如果未提供，则流会在流式传输时将表的最新快照返回为 `INSERT`，并将未来的更改返回为更改数据。 读取更改数据时还支持速率限制（`maxFilesPerTrigger`、`maxBytesPerTrigger`）和 `excludeRegex` 等选项。

``` scala 
// providing a starting version
spark.readStream.format("delta")
  .option("readChangeFeed", "true")
  .option("startingVersion", 0)
  .table("myDeltaTable")

// providing a starting timestamp
spark.readStream.format("delta")
  .option("readChangeFeed", "true")
  .option("startingVersion", "2021-04-21 05:35:43")
  .load("/pathToMyDeltaTable")

// not providing a starting version/timestamp will result in the latest snapshot being fetched first
spark.readStream.format("delta")
  .option("readChangeFeed", "true")
  .table("myDeltaTable")
```

### 5.5、 ChangeData Demo 

[获取笔记本](https://docs.microsoft.com/zh-cn/azure/databricks/_static/notebooks/delta/cdf-demo.html)

### 6、表实用工具命令

这里的官方文档就不往这里复制了，直接贴原文连接

> [表实用工具命令 - Azure Databricks - Workspace | Microsoft Docs](https://docs.microsoft.com/zh-cn/azure/databricks/delta/delta-utility)
>
> - [删除 Delta 表不再引用的文件](https://docs.microsoft.com/zh-cn/azure/databricks/delta/delta-utility#remove-files-no-longer-referenced-by-a-delta-table)
> - [检索 Delta 表历史记录](https://docs.microsoft.com/zh-cn/azure/databricks/delta/delta-utility#retrieve-delta-table-history)
> - [检索 Delta 表详细信息](https://docs.microsoft.com/zh-cn/azure/databricks/delta/delta-utility#retrieve-delta-table-details)
> - [将 Parquet 表转换为 Delta 表](https://docs.microsoft.com/zh-cn/azure/databricks/delta/delta-utility#convert-a-parquet-table-to-a-delta-table)
> - [将 Delta 表转换为 Parquet 表](https://docs.microsoft.com/zh-cn/azure/databricks/delta/delta-utility#convert-a-delta-table-to-a-parquet-table)
> - [将 Delta 表还原到早期状态](https://docs.microsoft.com/zh-cn/azure/databricks/delta/delta-utility#restore-a-delta-table-to-an-earlier-state)
> - [克隆 Delta 表](https://docs.microsoft.com/zh-cn/azure/databricks/delta/delta-utility#clone-a-delta-table)

### 7、并发控制

> [并发控制 - Azure Databricks - Workspace | Microsoft Docs](https://docs.microsoft.com/zh-cn/azure/databricks/delta/concurrency-control)

### 8、使用Delta Lake时的最佳做法

**提供数据位置提示**

如果希望在查询谓词中常规使用某一列，并且该列具有较高的基数（即包含多个非重复值），则使用 `Z-ORDER BY`。 Delta Lake 根据列值自动在文件中布局数据，在查询时根据布局信息跳过不相关的数据。

有关详细信息，请参阅 [Z 排序（多维聚类）](https://docs.microsoft.com/zh-cn/azure/databricks/delta/optimizations/file-mgmt#delta-zorder)。



**选择正确的分区列**

可以按列对 Delta 表进行分区。 最常使用的分区列是 `date`。 请按照以下两个经验法则来确定要根据哪个列进行分区：

1、如果某个列的基数将会很高，则不要将该列用于分区。 例如，如果你按列 `userId` 进行分区并且可能有 100 万个不同的用户 ID，则这是一种错误的分区策略。

2、每个分区中的数据量：如果你预计该分区中的数据至少有 1 GB，可以按列进行分区。

压缩文件

如果你连续将数据写入到 Delta 表，则它会随着时间的推移累积大量文件，尤其是小批量添加数据时。 这可能会对表读取效率产生不利影响，并且还会影响文件系统的性能。 理想情况下，应当将大量小文件定期重写到较小数量的较大型文件中。 这称为压缩。

你可以使用 [OPTIMIZE](https://docs.microsoft.com/zh-cn/azure/databricks/delta/optimizations/file-mgmt#delta-optimize) 命令来压缩表。



**替换表的内容或架构**

有时候，你可能希望替换 Delta 表。 例如： 。

- 你发现表中的数据不正确，需要对内容进行替换。
- 你希望重写整个表，以执行不兼容架构更改（删除列或更改列类型）。

尽管可以删除 Delta 表的整个目录并在同一路径上创建新表，但不建议这样做，因为：

- 删除目录效率不高。 删除某个包含极大文件的目录可能需要数小时甚至数天的时间。
- 删除的文件中的所有内容都会丢失；如果删除了错误的表，则很难恢复。
- 目录删除不是原子操作。 删除表时，某个读取表的并发查询可能会失败或看到的是部分表。

如果不需要更改表架构，则可以从 Delta 表中[删除](https://docs.microsoft.com/zh-cn/azure/databricks/delta/delta-update#delete-from-a-table)数据并插入新数据，或者通过[更新](https://docs.microsoft.com/zh-cn/azure/databricks/delta/delta-update#update-a-table)表来纠正不正确的值。

如果要更改表架构，则能够以原子方式替换整个表。 例如： 。

```SQL
SQL :
REPLACE TABLE <your-table> USING DELTA PARTITIONED BY (<your-partition-columns>) AS SELECT ... -- Managed table
REPLACE TABLE <your-table> USING DELTA PARTITIONED BY (<your-partition-columns>) LOCATION "<your-table-path>" AS SELECT ... -- External table
---------------------------------------------------------------------------
Scala: 
dataframe.write
  .format("delta")
  .mode("overwrite")
  .option("overwriteSchema", "true")
  .partitionBy(<your-partition-columns>)
  .saveAsTable("<your-table>") // Managed table
dataframe.write
  .format("delta")
  .mode("overwrite")
  .option("overwriteSchema", "true")
  .option("path", "<your-table-path>")
  .partitionBy(<your-partition-columns>)
  .saveAsTable("<your-table>") // External table
```

此方法有多个优点：

- 覆盖表的速度要快得多，因为它不需要以递归方式列出目录或删除任何文件。
- 表的旧版本仍然存在。 如果删除了错误的表，则可以使用[按时间顺序查看](https://docs.microsoft.com/zh-cn/azure/databricks/delta/delta-batch#query-an-older-snapshot-of-a-table-time-travel)轻松检索旧数据。
- 这是一个原子操作。 在删除表时，并发查询仍然可以读取表。
- 由于 Delta Lake ACID 事务保证，如果覆盖表失败，则该表将处于其以前的状态。

此外，如果你想要在覆盖表后删除旧文件以节省存储成本，则可以使用 [VACUUM](https://docs.microsoft.com/zh-cn/azure/databricks/delta/delta-utility#delta-vacuum) 来删除它们。 它针对文件删除进行了优化，通常比删除整个目录要快。



**Spark 缓存**

Databricks 建议使用 [Spark 缓存](https://docs.microsoft.com/zh-cn/azure/databricks/delta/optimizations/delta-cache#delta-and-rdd-cache-comparison)，如下所示

```python
df = spark.read.delta("/some/path")
df .cache()
```

仅适用于将多次使用一些消耗大量资源的聚合或联接结果（例如执行更多汇总）的情况。

否则，请不要对 Delta 表使用此方法。

- 丢失的任何数据跳过都可能归因于在缓存的 `DataFrame` 顶部添加的其他筛选器。
- 如果使用其他标识符访问表，则可能不会更新缓存的数据（例如，你执行 `spark.table(x).cache()`，但随后使用 `spark.write.save(/some/path)` 写入表）。



## 三、Delta Engine

>  Delta Engine 是与 Apache Spark 兼容的高性能查询引擎，提供了一种高效的方式来处理数据湖中的数据，包括存储在开源 Delta Lake 中的数据。 Delta Engine 优化可加快数据湖操作速度，并支持各种工作负载，从大规模 ETL 处理到临时交互式查询均可。 其中许多优化都自动进行；只需要通过将 Azure Databricks 用于数据湖即可获得这些 Delta Engine 功能的优势。

- 通过文件管理优化性能
  - [压缩（二进制打包）](https://docs.microsoft.com/zh-cn/azure/databricks/delta/optimizations/file-mgmt#compaction-bin-packing)
  - [跳过数据](https://docs.microsoft.com/zh-cn/azure/databricks/delta/optimizations/file-mgmt#data-skipping)
  - [Z 顺序（多维聚类）](https://docs.microsoft.com/zh-cn/azure/databricks/delta/optimizations/file-mgmt#z-ordering-multi-dimensional-clustering)
  - [调整文件大小](https://docs.microsoft.com/zh-cn/azure/databricks/delta/optimizations/file-mgmt#tune-file-size)
  - [Notebook](https://docs.microsoft.com/zh-cn/azure/databricks/delta/optimizations/file-mgmt#notebooks)
  - [提高交互式查询性能](https://docs.microsoft.com/zh-cn/azure/databricks/delta/optimizations/file-mgmt#improve-interactive-query-performance)
  - [常见问题解答 (FAQ)](https://docs.microsoft.com/zh-cn/azure/databricks/delta/optimizations/file-mgmt#frequently-asked-questions-faq)
- 自动优化
  - [自动优化的工作原理](https://docs.microsoft.com/zh-cn/azure/databricks/delta/optimizations/auto-optimize#how-auto-optimize-works)
  - [启用自动优化](https://docs.microsoft.com/zh-cn/azure/databricks/delta/optimizations/auto-optimize#enable-auto-optimize)
  - [何时选择加入和选择退出](https://docs.microsoft.com/zh-cn/azure/databricks/delta/optimizations/auto-optimize#when-to-opt-in-and-opt-out)
  - [示例工作流：使用并发删除或更新操作进行流式引入](https://docs.microsoft.com/zh-cn/azure/databricks/delta/optimizations/auto-optimize#example-workflow-streaming-ingest-with-concurrent-deletes-or-updates)
  - [常见问题解答 (FAQ)](https://docs.microsoft.com/zh-cn/azure/databricks/delta/optimizations/auto-optimize#frequently-asked-questions-faq)
- 通过缓存优化性能
  - [增量缓存和 Apache Spark 缓存](https://docs.microsoft.com/zh-cn/azure/databricks/delta/optimizations/delta-cache#delta-and-apache-spark-caching)
  - [增量缓存一致性](https://docs.microsoft.com/zh-cn/azure/databricks/delta/optimizations/delta-cache#delta-cache-consistency)
  - [使用增量缓存](https://docs.microsoft.com/zh-cn/azure/databricks/delta/optimizations/delta-cache#use-delta-caching)
  - [缓存一部分数据](https://docs.microsoft.com/zh-cn/azure/databricks/delta/optimizations/delta-cache#cache-a-subset-of-the-data)
  - [监视增量缓存](https://docs.microsoft.com/zh-cn/azure/databricks/delta/optimizations/delta-cache#monitor-the-delta-cache)
  - [配置增量缓存](https://docs.microsoft.com/zh-cn/azure/databricks/delta/optimizations/delta-cache#configure-the-delta-cache)
- [动态文件修剪](https://docs.microsoft.com/zh-cn/azure/databricks/delta/optimizations/dynamic-file-pruning)
- 隔离级别
  - [设置隔离级别](https://docs.microsoft.com/zh-cn/azure/databricks/delta/optimizations/isolation-level#set-the-isolation-level)
- Bloom 筛选器索引
  - [配置](https://docs.microsoft.com/zh-cn/azure/databricks/delta/optimizations/bloom-filters#configuration)
  - [创建 Bloom 筛选器索引](https://docs.microsoft.com/zh-cn/azure/databricks/delta/optimizations/bloom-filters#create-a-bloom-filter-index)
  - [删除 Bloom 筛选器索引](https://docs.microsoft.com/zh-cn/azure/databricks/delta/optimizations/bloom-filters#drop-a-bloom-filter-index)
  - [显示 Bloom 筛选器索引的列表](https://docs.microsoft.com/zh-cn/azure/databricks/delta/optimizations/bloom-filters#display-the-list-of-bloom-filter-indexes)
  - [笔记本](https://docs.microsoft.com/zh-cn/azure/databricks/delta/optimizations/bloom-filters#notebook)
- 优化联接性能
  - [范围联接优化](https://docs.microsoft.com/zh-cn/azure/databricks/delta/join-performance/range-join)
  - [倾斜联接优化](https://docs.microsoft.com/zh-cn/azure/databricks/delta/join-performance/skew-join)
- 优化的数据转换
  - [高阶函数](https://docs.microsoft.com/zh-cn/azure/databricks/delta/data-transformation/higher-order-lambda-functions)
  - [转换复杂数据类型](https://docs.microsoft.com/zh-cn/azure/databricks/delta/data-transformation/complex-types)

### 1、通过文件管理优化性能

> 为提高查询速度，Azure Databricks 上的 Delta Lake 支持优化存储在云存储中的数据的布局。 Azure Databricks 上的 Delta Lake 支持两种布局算法：二进制打包和 Z 排序。
>
> 本文介绍如何运行优化命令、两种布局算法的工作原理以及如何清理过时的表快照。
>
> - [常见问题解答](https://docs.microsoft.com/zh-cn/azure/databricks/delta/optimizations/file-mgmt#optimize-faq)阐释了为什么优化不是自动进行的，并提供了有关运行优化命令的频率的建议。
> - 有关演示优化优点的笔记本，请参阅[优化示例](https://docs.microsoft.com/zh-cn/azure/databricks/delta/optimizations/optimization-examples)。

#### 1.1、压缩（二进制打包）

> Azure Databricks 上的 Delta Lake 可以将小文件合并为较大的文件，从而提高表中读取查询的速度。 通过运行 `OPTIMIZE` 命令触发压缩：
>
> ```SQL 
> OPTIMIZE delta.`/data/events`
> 或
> OPTIMIZE events
> 
> 如果拥有大量数据，并且只想要优化其中的一个子集，则可以使用 WHERE 指定一个可选的分区谓词：
> OPTIMIZE events WHERE date >= '2017-01-01'
> ```

#### 1.2、跳过数据

> 向 Delta 表中写入数据时，会自动收集跳过数据信息。 Azure Databricks 上的 Delta Lake 会在查询时利用此信息（最小值和最大值）来提供更快的查询。 不需要配置跳过的数据；此功能会在适用时激活。 但其有效性取决于数据的布局。 为了获取最佳结果，请应用 [Z 排序](https://docs.microsoft.com/zh-cn/azure/databricks/delta/optimizations/file-mgmt#delta-zorder)。
>
> 详细了解 Azure Databricks 上的 Delta Lake 跳过数据和 Z 排序的优点，请参阅[优化示例](https://docs.microsoft.com/zh-cn/azure/databricks/delta/optimizations/optimization-examples)中的笔记本。 默认情况下，Azure Databricks 上的 Delta Lake 收集你的表架构中定义的前 32 列的统计信息。 你可以使用[表属性](https://docs.microsoft.com/zh-cn/azure/databricks/delta/delta-batch#table-properties) `dataSkippingNumIndexedCols` 来更改此值。 在写入文件时，添加更多的列来收集统计信息会增加额外的开销。
>
> 为了收集统计信息，嵌套列中的每个字段都被视为单独的列。
>
> 有关详细信息，请参阅博客文章：[通过 Databricks Delta 以在数秒内处理数 PB 的数据](https://databricks.com/blog/2018/07/31/processing-petabytes-of-data-in-seconds-with-databricks-delta.html)。

#### 1.3、Z排序（多位聚类）

> Z 排序是并置同一组文件中相关信息的[方法](https://en.wikipedia.org/wiki/Z-order_curve)。 Azure Databricks 上的 Delta Lake 数据跳过算法会自动使用此并置，大幅减少需要读取的数据量。 对于 Z 排序数据，请在 `ZORDER BY` 子句中指定要排序的列：
>
> ```sql
> OPTIMIZE events
> WHERE date >= current_timestamp() - INTERVAL 1 day
> ZORDER BY (eventType)
> ```
>
> 如果希望在查询谓词中常规使用某一列，并且该列具有较高的基数（即包含多个非重复值），请使用 `ZORDER BY`。
>
> 可以将 `ZORDER BY` 的多个列指定为以逗号分隔的列表。 但是，区域的有效性会随每个附加列一起删除。 Z 排序对于未收集统计信息的列无效并且会浪费资源，因为需要列本地统计信息（如最小值、最大值和总计）才能跳过数据。 可以通过对架构中的列重新排序或增加从中收集统计信息的列数，对某些列配置统计信息收集。 （有关详细信息，请参阅[跳过数据](https://docs.microsoft.com/zh-cn/azure/databricks/delta/optimizations/file-mgmt#delta-data-skipping)部分）。
>
>  备注
>
> - Z 排序不是幂等的，而应该是增量操作。 多次运行不能保证 Z 排序所需的时间减少。 但是，如果没有将新数据添加到刚刚进行 Z 排序的分区，则该分区的另一个 Z 排序将不会产生任何效果。
>
> - Z 排序旨在根据元组的数量生成均匀平衡的数据文件，但不一定是磁盘上的数据大小。 这两个度量值通常是相关的，但可能会有例外的情况，导致优化任务时间出现偏差。
>
>   例如，如果 `ZORDER BY` 日期，并且最新记录的宽度比过去多很多（例如数组或字符串值较长），则 `OPTIMIZE` 作业的任务持续时间和所生成文件的大小都会出现偏差。 但这只是 `OPTIMIZE` 命令本身的问题；它不应对后续查询产生任何负面影响。

#### 1.4、调整文件大小

> 本部分介绍了如何调整 Delta 表中文件的大小。
>
> - [设置目标大小](https://docs.microsoft.com/zh-cn/azure/databricks/delta/optimizations/file-mgmt#set-a-target-size)
> - [基于工作负荷自动调整](https://docs.microsoft.com/zh-cn/azure/databricks/delta/optimizations/file-mgmt#autotune-based-on-workload)
> - [基于表大小自动调整](https://docs.microsoft.com/zh-cn/azure/databricks/delta/optimizations/file-mgmt#autotune-based-on-table-size)

Databricks 上的 Delta Lake 优化 Scala 笔记本  [获取笔记本](https://docs.microsoft.com/zh-cn/azure/databricks/_static/notebooks/delta/optimize-scala.html)

Databricks 上的 Delta Lake 优化 SQL 笔记本  [获取笔记本](https://docs.microsoft.com/zh-cn/azure/databricks/_static/notebooks/delta/optimize-sql.html)

#### 1.5、 提高交互式查询性能

> Delta Engine 提供了一些额外的机制来提高查询性能。
>
> - [管理数据时效性](https://docs.microsoft.com/zh-cn/azure/databricks/delta/optimizations/file-mgmt#manage-data-recency)
> - [用于低延迟查询的增强检查点](https://docs.microsoft.com/zh-cn/azure/databricks/delta/optimizations/file-mgmt#enhanced-checkpoints-for-low-latency-queries)
>
> Delta Lake 不会执行昂贵的 JSON 分析来获取列级统计信息。
>
> Parquet 列修剪功能可以显著减少读取列的统计信息所需的 I/O。
>
> 结构格式启用一系列优化，这些优化可以将增量 Delta Lake 读取操作的开销从数秒降低到数十毫秒，大大降低短查询的延迟。



### 2、自动优化

> 自动优化是一组可选功能，可在向 Delta 表进行各次写入时自动压缩小文件。 在写入过程中支付少量的开销可以为进行频繁查询的表带来显著的好处。 自动优化在下列情况下尤其有用：
>
> - 可以接受分钟级延迟的流式处理用例
> - `MERGE INTO` 是写入到 Delta Lake 的首选方法
> - `CREATE TABLE AS SELECT` 或 `INSERT INTO` 是常用操作

#### 2.1、自动优化的工作原理

> 自动优化包括两个互补功能：优化写入和自动压缩。

- 优化写入如何工作：Azure Databricks 基于实际数据动态优化 Apache Spark 分区大小，并尝试为每个表分区写出 128 MB 的文件。 这是一个近似大小，可能因数据集特征而异。
- 自动压缩如何工作：在每次写入后，Azure Databricks 会检查文件是否可以进一步压缩，并运行一个 OPTIMIZE 作业（128 MB 文件大小，而不是标准 OPTIMIZE 中使用的 1 GB 文件大小），以便进一步压缩包含最多小文件的分区的文件。

![优化写入](https://docs.microsoft.com/zh-cn/azure/databricks/_static/images/delta/optimized-writes.png)

#### 2.2、启用自动优化

> 必须使用以下方法之一显式启用优化写入和自动压缩：
>
> - **新表**：在 `delta.autoOptimize.autoCompact = true` 命令中设置表属性 `delta.autoOptimize.optimizeWrite = true` 和 `CREATE TABLE`。
>
>   ```sql
>   CREATE TABLE student (id INT, name STRING, age INT) TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)
>   ```
>
> - **现有表**：在 `delta.autoOptimize.autoCompact = true` 命令中设置表属性 `delta.autoOptimize.optimizeWrite = true` 和 `ALTER TABLE`。
>
>   ```sql
>   ALTER TABLE [table_name | delta.`<table-path>`] SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)
>   ```
>
> - **所有新表**：
>
>   ```sql
>   set spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite = true;
>   set spark.databricks.delta.properties.defaults.autoOptimize.autoCompact = true;
>   ```
>
> 此外，可以使用以下配置为 Spark 会话启用和禁用这两项功能：
>
> - `spark.databricks.delta.optimizeWrite.enabled`
> - `spark.databricks.delta.autoCompact.enabled`
>
> 会话配置优先于表属性，因此你可以更好地控制何时选择加入或选择退出这些功能。

#### 2.3、合适选择加入和选择退出

本部分提供了有关何时选择加入和选择退出自动优化功能的指导。

##### 2.3.1、何时选择加入优化写入

优化写入旨在最大程度地提高写入到存储服务的数据的吞吐量。 这可以通过减少写入的文件数来实现，而不会牺牲过多的并行度。

优化写入需要根据目标表的分区结构来混排数据。 这种混排自然会产生额外成本。 但是，写入过程中的吞吐量增加可能会对冲掉混排成本。 如果没有，查询数据时吞吐量会增加，因此这个功能仍然是很有价值的。

优化写入的关键在于它是一个自适应的混排。 如果你有一个流式处理引入用例，并且输入数据速率随时间推移而变化，则自适应混排会根据不同微批次的传入数据速率自行进行相应调整。 如果代码片段在写出流之前执行 `coalesce(n)` 或 `repartition(n)`，则可以删除这些行。

**何时选择加入**

- 可以接受分钟级延迟的流式处理用例
- 使用 `MERGE`、`UPDATE`、`DELETE`、`INSERT INTO`、`CREATE TABLE AS SELECT` 之类的 SQL 命令时

**何时选择退出**

当写入的数据以兆字节为数量级且存储优化实例不可用时。

##### 2.3.2、何时选择加入自动压缩

自动压缩发生在向表进行写入的操作成功后，在执行了写入操作的群集上同步运行。 这意味着，如果你的代码模式向 Delta Lake 进行写入，然后立即调用 `OPTIMIZE`，则可以在启用自动压缩的情况下删除 `OPTIMIZE` 调用。

自动压缩使用与 `OPTIMIZE` 不同的试探法。 由于它在写入后同步运行，因此我们已将自动压缩功能优化为使用以下属性运行：

- Azure Databricks 不支持将 Z 排序与自动压缩一起使用，因为 Z 排序的成本要远远高于纯压缩。
- 自动压缩生成比 `OPTIMIZE` (1 GB) 更小的文件 (128 MB)。
- 自动压缩“贪婪地”选择能够最好地利用压缩的有限的一组分区。 所选的分区数因其启动时所处的群集的大小而异。 如果群集有较多的 CPU，则可以优化较多的分区。
- 若要控制输出文件大小，请设置 [Spark 配置](https://docs.microsoft.com/zh-cn/azure/databricks/clusters/configure#spark-config) `spark.databricks.delta.autoCompact.maxFileSize`。 默认值为 `134217728`，该值会将大小设置为 128 MB。 指定值 `104857600` 会将文件大小设置为 100 MB。

**何时选择加入**

- 可以接受分钟级延迟的流式处理用例
- 当表上没有常规 `OPTIMIZE` 调用时

**何时选择退出**

当其他编写器可能同时执行 `DELETE`、`MERGE`、`UPDATE` 或 `OPTIMIZE` 之类的操作时（因为自动压缩可能会导致这些作业发生事务冲突）。 如果自动压缩由于事务冲突而失败，Azure Databricks 不会使压缩失败，也不会重试压缩。

##### 2.3.3、示例工作流：使用并发删除或更新操作进行流式引入

此工作流假定你有一个群集运行全天候流式处理作业来引入数据，另一个群集每小时、每天或临时运行一次作业来删除或更新一批记录。 对于此用例，Azure Databricks 建议你：

- 使用以下命令在表级别启用优化写入

  ```sql
  ALTER TABLE <table_name|delta.`table_path`> SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true)
  ```

  这可以确保流写入的文件数以及删除和更新作业的大小是最佳的。

- 对执行删除或更新操作的作业使用以下设置将在会话级别启用自动压缩。

  ```scala
  spark.sql("set spark.databricks.delta.autoCompact.enabled = true")
  ```

  这样就可以在表中压缩文件。 由于这发生在删除或更新之后，因此可以减轻事务冲突的风险。



### 3、通过缓存优化性能

> [通过缓存优化性能 - Azure Databricks - Workspace | Microsoft Docs](https://docs.microsoft.com/zh-cn/azure/databricks/delta/optimizations/delta-cache)

> 增量缓存通过使用快速中间数据格式在节点的本地存储中创建远程文件的副本来加快数据读取。 必须从远程位置提取文件时，会自动缓存数据。 然后在本地连续读取上述数据，这会显著提高读取速度。
>
> 增量缓存适用于所有 Parquet 文件，并且不限于 [Delta Lake 格式的文件](https://docs.microsoft.com/zh-cn/azure/databricks/delta/delta-faq)。 增量缓存支持读取 DBFS、HDFS、Azure Blob 存储、Azure Data Lake Storage Gen1 和 Azure Data Lake Storage Gen2 中的 Parquet 文件。 它不支持其他存储格式，例如 CSV、JSON 和 ORC。
>
> 相关章节内容：
>
> 1. [增量缓存和 Apache Spark 缓存](https://docs.microsoft.com/zh-cn/azure/databricks/delta/optimizations/delta-cache#delta-and-apache-spark-caching)
> 2. [增量缓存一致性](https://docs.microsoft.com/zh-cn/azure/databricks/delta/optimizations/delta-cache#delta-cache-consistency)
> 3. [使用增量缓存](https://docs.microsoft.com/zh-cn/azure/databricks/delta/optimizations/delta-cache#use-delta-caching)
> 4. [缓存数据的子集](https://docs.microsoft.com/zh-cn/azure/databricks/delta/optimizations/delta-cache#cache-a-subset-of-the-data)
> 5. [监视增量缓存](https://docs.microsoft.com/zh-cn/azure/databricks/delta/optimizations/delta-cache#monitor-the-delta-cache)
> 6. [配置增量缓存](https://docs.microsoft.com/zh-cn/azure/databricks/delta/optimizations/delta-cache#configure-the-delta-cache)

下表总结了增量缓存和 Apache Spark 缓存之间的主要区别，以便选择最适合工作流的工具：

| 功能     | 增量缓存                                               | Apache Spark 缓存                                |
| :------- | :----------------------------------------------------- | :----------------------------------------------- |
| 存储格式 | 工作器节点上的本地文件。                               | 内存中块，但取决于存储级别。                     |
| 适用对象 | WASB 和其他文件系统上存储的任何 Parquet 表格。         | 任何 DataFrame 或 RDD。                          |
| 触发     | 自动执行，第一次读取时（如果启用了缓存）。             | 手动执行，需要更改代码。                         |
| 已评估   | 惰性。                                                 | 惰性。                                           |
| 强制缓存 | `CACHE` 和 `SELECT`                                    | `.cache` + 任何实现缓存的操作和 `.persist`。     |
| 可用性   | 可以使用配置标志启用或禁用，可以在某些节点类型上禁用。 | 始终可用。                                       |
| 逐出     | 更改任何文件时自动执行，重启群集时手动执行。           | 以 LRU 方式自动执行，使用 `unpersist` 手动执行。 |

### 4、动态文件修剪

> 动态文件修剪 (DFP) 可以显著提高 Delta 表上许多查询的性能。 DFP 对于非分区表或非分区列上的联接特别有效。 DFP 的性能影响通常与数据聚类相关，因此请考虑使用 [Z 排序](https://docs.microsoft.com/zh-cn/azure/databricks/delta/optimizations/file-mgmt#delta-zorder)以最大限度地提高 DFP 的效益。
>
> 有关 DFP 的背景和用例，请参阅[通过动态文件修剪在 Delta Lake 上更快进行 SQL 查询](https://databricks.com/blog/2020/04/30/faster-sql-queries-on-delta-lake-with-dynamic-file-pruning.html)。

### 5、隔离级别

> 表的隔离级别定义了必须将某事务与并发事务所作修改进行隔离的程度。 Azure Databricks 上的 Delta Lake 支持两个隔离级别：Serializable 和 WriteSerializable。
>
> - **Serializable**：最强隔离级别。 它可确保提交的写入操作和所有读取均[可序列化](https://en.wikipedia.org/wiki/Serializability)。 只要有一个串行序列一次执行一项操作，且生成与表中所示相同的结果，则可执行这些操作。 对于写入操作，串行序列与表的历史记录中所示完全相同。
>
> - **WriteSerializable（默认）** ：强度比 Serializable 低的隔离级别。 它仅确保写入操作（而非读取）可序列化。 但是，这仍比[快照](https://en.wikipedia.org/wiki/Snapshot_isolation)隔离更安全。 WriteSerializable 是默认的隔离级别，因为对大多数常见操作而言，它使数据一致性和可用性之间达到良好的平衡。
>
>   在此模式下，Delta 表的内容可能与表历史记录中所示的操作序列不同。 这是因为此模式允许某些并发写入对（例如操作 X 和 Y）继续执行，这样的话，即使历史记录显示在 X 之后提交了 Y，结果也像在 X 之前执行 Y 一样（即它们之间是可序列化的）。若要禁止这种重新排序，请将[表隔离级别设置](https://docs.microsoft.com/zh-cn/azure/databricks/delta/optimizations/isolation-level#setting-isolation-level)为 Serializable，以使这些事务失败。
>
> 读取操作始终使用快照隔离。 写入隔离级别确定读取者是否有可能看到某个“从未存在”（根据历史记录）的表的快照。
>
> 对于 Serializable 级别，读取者始终只会看到符合历史记录的表。 对于 WriteSerializable 级别，读取者可能会看到在增量日志中不存在的表。
>
> 例如，请考虑 txn1（长期删除）和 txn2（它插入 txn1 删除的数据）。 txn2 和 txn1 完成，并且它们会按照该顺序记录在历史记录中。 根据历史记录，在 txn2 中插入的数据在表中不应该存在。 对于 Serializable 级别，读取者将永远看不到由 txn2 插入的数据。 但是，对于 WriteSerializable 级别，读取者可能会在某个时间点看到由 txn2 插入的数据。
>
> 若要详细了解哪些类型的操作可能会在每个隔离级别彼此冲突以及可能出现的错误，请参阅[并发控制](https://docs.microsoft.com/zh-cn/azure/databricks/delta/concurrency-control)。
>
> ```sql 
> - 设置隔离级别
> 
> 使用 `ALTER TABLE` 命令设置隔离级别。
> ALTER TABLE <table-name> SET TBLPROPERTIES ('delta.isolationLevel' = <level-name>)
> 
> 其中，`<level-name>` 是 `Serializable` 或 `WriteSerializable`。
> 
> 例如，若要将隔离级别从默认的 `WriteSerializable` 更改为 `Serializable`，请运行：
> ALTER TABLE <table-name> SET TBLPROPERTIES ('delta.isolationLevel' = 'Serializable')
> 
> ```

### 6、Bloom筛选器索引

> [Bloom 筛选器](https://en.wikipedia.org/wiki/Bloom_filter)索引是一种节省空间的数据结构，可跳过选定列中的数据（特别对于包含任意文本的字段）。 Bloom 筛选器的工作方式是：声明数据肯定不在文件中或可能在文件中，并定义误报率 (FPP) 。
>
> **配置：**
>
> 默认启用 Bloom 筛选器。 若要禁用 Bloom 筛选器，请将会话级别 `spark.databricks.io.skipping.bloomFilter.enabled` 配置设置为 `false`。
>
> 以下笔记本演示了定义 Bloom 筛选器索引如何加速“大海捞针式”查询。
>
> [Bloom filter index demo - Databricks (microsoft.com)](https://docs.microsoft.com/zh-cn/azure/databricks/_static/notebooks/delta/bloom-filter-index-demo.html)

### 7、优化联接性能

Azure Databricks 上的 Delta Lake 优化了范围和倾斜联接。 范围联接优化要求基于查询模式进行优化，倾斜联接可以通过倾斜提示提高效率。 请参阅以下文章，了解如何对这些联接优化进行最佳利用：

[优化联接性能 - Azure Databricks - Workspace | Microsoft Docs](https://docs.microsoft.com/zh-cn/azure/databricks/delta/join-performance/)

### 8、优化的数据转换

Azure Databricks 使用嵌套类型优化高阶函数和数据帧操作的性能。

> - [高阶函数](https://docs.microsoft.com/zh-cn/azure/databricks/delta/data-transformation/higher-order-lambda-functions)
>
> zure Databricks 提供了专用的基元，用于处理 Apache Spark SQL 中的数组；这使得使用数组变得更容易、更简洁，并省去了通常需要的大量样板代码。 基元围绕两个函数式编程构造：高阶函数和匿名 (lambda) 函数。 这两种函数协同工作，让你能够定义在 SQL 中操作数组的函数。 高阶函数采用一个数组，实现数组的处理方式以及计算结果。 它委托 lambda 函数处理数组中的每一项。
>
> 高阶函数笔记本简介: [获取笔记本](https://docs.microsoft.com/zh-cn/azure/databricks/_static/notebooks/higher-order-functions.html)
>
> 高阶函数教程 Python 笔记本[获取笔记本](https://docs.microsoft.com/zh-cn/azure/databricks/_static/notebooks/higher-order-functions-tutorial-python.html)
>
> - [转换复杂数据类型](https://docs.microsoft.com/zh-cn/azure/databricks/delta/data-transformation/complex-types)
>
> 处理嵌套数据类型时，Azure Databricks 上的 Delta Lake 对某些现成的转换进行了优化。 以下笔记本中包含了很多示例来说明如何使用 Apache Spark SQL 中本机支持的函数在复杂数据类型和基元数据类型之间进行转换。
>
> 转换复杂数据类型 - Scala 笔记本[获取笔记本](https://docs.microsoft.com/zh-cn/azure/databricks/_static/notebooks/transform-complex-data-types-scala.html)
>
> 转换复杂数据类型 - SQL 笔记本 [获取笔记本](https://docs.microsoft.com/zh-cn/azure/databricks/_static/notebooks/transform-complex-data-types-sql.html)

### 9、优化示例

Databricks 上的 Delta Lake 优化 Scala 笔记  [获取笔记本](https://docs.microsoft.com/zh-cn/azure/databricks/_static/notebooks/delta/optimize-scala.html)

Databricks 上的 Delta Lake 优化 SQL 笔记本 [获取笔记本](https://docs.microsoft.com/zh-cn/azure/databricks/_static/notebooks/delta/optimize-sql.html)



## 四、开发人员工具和指南

### 1、使用IDE

Idea？

### 2、使用数据库工具

> datagrip连接databricks。   根据官方文档已经连接成功啦
>
> [DataGrip 与 Azure Databricks 的集成 - Azure Databricks - Workspace | Microsoft Docs](https://docs.microsoft.com/zh-cn/azure/databricks/dev-tools/datagrip)











