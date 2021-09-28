title: Databricks

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

##### 3.2.1、 追加模式

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

























