# 功能点

## 关注的维度（主要关注边界和典型）

### 配置文件输入
#### 认证参数的检查
- endpoint的检查，主要包括合法性检查，如是否以http开头等
- accessid和accesskey的检查，主要包括值的两边或者一边包括空字符的情况
- instaneName的检查，主要的检查版本不是internal的情况，空字符串、最大的字符串

#### Table名称的检查
- 两边有空格
- 带上了instanceId的表名
- 空字符串，最大字符串

### PK的检查
- 空数组，1个PK，4个PK，5个PK，100个Attr
- 合法的PK类型，非法的PK类型
- 合法的参数构造，非法的参数构造
- 合法ColumnName的长度检查，非法长度ColumnName的长度检查

### Column的检查
#### 普通模式
- 空数组，1个Attr，8个Attr，128个Attr，129个Attr，1000个Attr
- 合法的参数构造，非法的参数构造
- 合法ColumnName的长度检查，非法长度ColumnName的长度检查

#### 多版本模式
- 空数组，1个Attr，8个Attr，128个Attr，129个Attr，1000个Attr
- 合法的参数构造，非法的参数构造
- 合法ColumnName的长度检查，非法长度ColumnName的长度检查

### Mode的检查
- 合法的参数构造，非法的参数构造
- 检查具体的Mode是否和Column匹配

### writeMode的检查
- 合法的参数构造，非法的参数构造
- 检查具体的writeMode是否和Mode匹配(多版本模式下只支持UpdateRow的模式)

### Datax输入
#### 标准模式的输入检查
- 构造PK分别有1、2、4列的情况，且每列都分别有随机指定PK列的类型
- 构造PK为4列的情况，属性列分别为1、5、128、129、1000的情况
- Datax输入和用户指定不匹配的情况

#### 多版本模式的输入检查
- 构造PK分别有1、2、4列的情况，且每列都分别有随机指定PK列的类型
- Datax传入的格式和用户不匹配
- PK列有部分和全部不能转换为指定类型
- PK列有空列
- ColumnName不能转换位字符串，ColumnName不再用户的配置列表中
- TS不能转换为数字类型，TS为空
- Value不能转换为指定的类型，Value为空
- 构造一行数据，构造不同列的Cell，相同版本，Cell个数为：1，10，128，129，1000
- 构造一行数据，构造不同列的Cell，不同版本，Cell个数为：1，10，128，129，1000
- 构造一行数据，构造同列的Cell，不同版本，Cell个数为：1，10，128，129，1000
- 构造一行数据，构造同列的Cell，同版本，Cell个数为：1，10，128，129，1000
- 构造N个相同行的数据，每行一个Column（一个Cell，相同版本）且每行的Column不相同
- 构造N个相同行的数据，每行一个Column（一个Cell，相同版本）且每行的Column相同
- 构造N个相同行的数据，每行一个Column（一个Cell，不同版本）且每行的Column相同
- 构造N个相不同行的数据，N分别为1、10、100、200

## Failover（E2E）

# 测试用例

## 参数检测测试

- DONE 测试目的：测试正常逻辑的检测是否符合预期。测试内容：构造一个合法的配置文件，期望配置文件解析正确，且参数符合预期(包括对默认参数的检查)
- DONE 测试目的：测试必填参数的存在性检查是否符合预期。测试内容：分别不构造endpoint、accessId、accessKey、instanceName、table、primaryKey、column、writeMode、mode，期望解析出错，且错误消息符合预期
- DONE 测试目的：测试参数值是否为空得检查是否符合预期。测试内容：分别构造endpoint、accessId、accessKey、instanceName、table、primaryKey、column、writeMode、mode的值为空（空字符串或者空数组），期望解析出错，且错误消息符合预期
- DONE 测试目的：测试参数非法的参数检测是否符合预期。测试内容：构造一个不是以http开头的endpoint ，期望解析出错
- DONE 测试目的：测试accessid和accesskey两边有空字符串的解析情况。测试内容：分别构造两边带空字符串的accessid和accesskey，期望程序解析正确。
- DONE 测试目的：测试instanceName版本不匹配程序的解析情况。分别构造一个Public和Legacy的instanceName，期望程序解析出错
- DONE 测试目的：测试table解析情况。测试内容：构造两边带空字符串的table，期望程序解析正确。在表前添加instanceId，期望解析出错。
- DONE 测试目的：测试primaryKey和column中有重复列名检查是否符合预期。测试内容：分别在primaryKey和column都构造同名的列，期望解析出错，且错误消息符合预期
- DONE 测试目的：测试writeMode值解析是否符合预期。测试内容：分别构造writeMode的值为：PutRow、UpdateRow、putrow、updaterow、PUTROW、UPDATEROW，期望解析正常，值符合预期。分别构造writeMode的值为put、PUT、put-row、update，期望解析出错，错误消息符合预期
- DONE 测试目的：测试mode值解析是否符合预期。测试内容：分别构造mode的值为：multiversion、MultiVersion、multiVERSION、MULTIVERSION、normal、NORMAL、Normal，期望解析正常，值符合预期。分别构造mode的值为multi、version、normalVersion，期望解析错误，且错误消息符合预期
- DONE 测试目的：测试默认值是否符合预期。测试内容：分别为默认参数构造特定的值，期望值符合预期。
- DONE 测试目的：测试primaryKey的值合法性检查是否符合预期。测试内容：分别构造
{\"type\":\"String\"}、
{\"name\":\"Uid\"}、
{}、
{\"name\":\"Uid\",\"type\":\"String\", \"value\":\"\"}、
{\"name\":\"Uid\", \"type\":\"Integer\"}、
{\"name\":\"\", \"type\":\"String\"}、
{\"name\":\"UID\",\"type\":\"String\"}{\"name\":\"UID\",\"type\":\"String\"}、
{\"name\":\"UID\",\"type\":\"Bool\"}、
{\"name\":\"UID\",\"type\":\"double\"}、
{\"name\":\"Uid\",\"type\":\"string\"}、
{\"name\":\"Uid\",\"type\":\"string\"}{\"name\":\"Pid\",\"type\":\"int\"}{\"name\":\"Mid\",\"type\":\"int\"}{\"name\":\"UID\",\"type\":\"string\"}{\"name\":\"xx\",\"type\":\"string\"}、
100个PK列
期望primaryKey解析出错，错误消息符合预期。
- DONE 测试目的：测试在多版本模式下，对PutRow的检查是否生效。测试内容：配置多版本，但是指定写入模式为PutRow，期望检查出错，且错误消息符合预期
- DONE 测试目的：测试column的值合法性检查是否符合预期。测试内容：构造128列属性列，期望解析配置正确。构造129列属性列，期望解析配置出错，且错误消息符合预期。
- DONE 测试目的：测试tableName长度超过预期的行为是否符合预期。测试内容：构造一个长度为max的表，输入插件中，期望解析正常。输入一个max+1的表名，期望解析错误。
- DONE 测试目的：测试mode和Column使用错误的情况是否符合预期。测试内容：指定mode为multiVersion但是Column为普通模式，指定mode为normal但是Column为多版本模式，期望解析错误。

### 普通模式下的参数测试
- DONE 测试目的：测试column的值合法性检查是否符合预期。测试内容：分别构造
{\"type\":\"String\"}、
{\"name\":\"Uid\"}、
{}、
{\"name\":\"Uid\",\"type\":\"String\", \"value\":\"\"}、
{\"name\":\"Uid\", \"type\":\"Integer\"}、
{\"name\":\"\", \"type\":\"String\"}、
期望column解析出错，错误消息符合预期。
分别构造1、8、128个Column，期望解析正确。
构造129、1000个Column，期望解析错误。

### 多版本模式下的参数测试
- DONE 测试目的：测试column的值合法性检查是否符合预期。测试内容：分别构造
{\"type\":\"String\"}、
{\"name\":\"Uid\"}、
{}、
{\"name\":\"Uid\", \"type\":\"String\", \"value\":\"\"}、
{\"name\":\"Uid\", \"type\":\"Integer\"}、
{\"name\":\"\",\"type\":\"String\"}、
{\"name\":\"attr_0\", \"type\":\"String\"}、
{\"srcName\":\"Uid\", \"name\":\"Uid\",\"type\":\"Integer\"}、
{\"srcName\":\"Uid\", \"name\":\"\",  \"type\":\"String\"}、
{\"srcName\":\"Uid\", \"name\":\"old\",  \"type\":\"String\"}{\"srcName\":\"Uid\", \"name\":\"new\",\"type\":\"Int\"}、
{\"srcName\":\"old\", \"name\":\"ss\",  \"type\":\"String\"}{\"srcName\":\"new\", \"name\":\"ss\", \"type\":\"String\"}、
期望column解析出错，错误消息符合预期。
分别构造1、8、128个Column，期望解析正确。
构造129、1000个Column，期望解析错误。

## 类型转换测试
- DONE 测试目的：测试所有类型转为string是否符合预期。测试内容：分别构造string、integer、binary、bool、double转换为string，期望值符合预期
- DONE 测试目的：测试所有类型转为integer是否符合预期。测试内容：分别构造string、integer、binary、bool、double转换为integer，期望值符合预期
- DONE 测试目的：测试所有类型转为binary是否符合预期。测试内容：分别构造string、integer、binary、bool、double转换为binary，期望值符合预期
- DONE 测试目的：测试所有类型转为bool是否符合预期。测试内容：分别构造string、integer、binary、bool、double转换为bool，期望值符合预期
- DONE 测试目的：测试所有类型转为double是否符合预期。测试内容：分别构造string、integer、binary、bool、double转换为double，期望值符合预期

## CU测试

## 普通模式数据导入测试

### 多PK混合类型测试
- DONE 测试目的：测试在多PK和不同类型的组合下程序是否符合预期。测试内容，分别构造PK列为（1列PK）(string)、(integer)、(binary)、（2列PK）(string、integer)、（integer、binary）、（binary、string），（4列PK）（string，string，integer，binary）、（integer、string、binary，string）、（binary、string，integer、integer），期望数据符合预期。

### PutRow模式写入测试
- DONE 测试目的：测试在PutRow模式下，数据是否能正常的导入OTS中。测试内容：创建一个拥有1个PK的表，并分别构造1、10、50、100、500不重复的行，导入OTS，期望数据符合预期
- DONE 测试目的：测试在PutRow模式下，数据是否能正常的导入OTS中。测试内容：创建一个拥有1个PK的表，并分别构造10、50、100、500重复的行，导入OTS，期望数据符合预期
- DONE 测试目的：测试在PutRow模式下，数据是否能正常的导入OTS中。测试内容：创建一个拥有4个PK的表，并分别构造1、10、50、100、500不重复的行，导入OTS，期望数据符合预期
- DONE 测试目的：测试在PutRow模式下，数据是否能正常的导入OTS中。测试内容：创建一个拥有4个PK的表，并分别构造10、50、100、500重复的行，导入OTS，期望数据符合预期

### UpdateRow模式写入测试
- DONE 测试目的：测试在UpdateRow模式下，数据是否能正常的导入OTS中。测试内容：创建一个拥有1个PK的表，并分别构造1、10、50、100、500不重复的行，导入OTS，期望数据符合预期
- DONE 测试目的：测试在UpdateRow模式下，数据是否能正常的导入OTS中。测试内容：创建一个拥有1个PK的表，并分别构造10、50、100、500重复的行，导入OTS，期望数据符合预期
- DONE 测试目的：测试在UpdateRow模式下，数据是否能正常的导入OTS中。测试内容：创建一个拥有4个PK的表，并分别构造1、10、50、100、500不重复的行，导入OTS，期望数据符合预期
- DONE 测试目的：测试在UpdateRow模式下，数据是否能正常的导入OTS中。测试内容：创建一个拥有4个PK的表，并分别构造10、50、100、500重复的行，导入OTS，期望数据符合预期

### 数据异常测试
- DONE 测试目的：测试datax传入了不符合预期的数据的ots-writer的行为表现是否符合预期。测试内容：用户配置了5列，但是datax给了4列，期望writer异常退出，错误消息符合预期
- DONE 测试目的：测试datax传入了不符合预期的数据的ots-writer的行为表现是否符合预期。测试内容：用户配置了5列，但是datax给了6列，期望writer异常退出，错误消息符合预期
- DONE 测试目的：测试datax传入了不符合预期的数据的ots-writer的行为表现是否符合预期。测试内容：构造了10行数据，但是其中有一行数据的PK列为空，期望该行数据被记录到脏数据回收器中，错误消息符合预期
- DONE 测试目的：测试datax传入了不符合预期的数据的ots-writer的行为表现是否符合预期。测试内容：构造了10行数据，但是其中有一行数据不能成功转换为指定的类型，期望该行数据被记录到脏数据回收器中，错误消息符合预期

### 限制项测试
- DONE 测试目的：测试限制项对writer的行为影响是否符合预期。测试内容：构造string的pk列，值长度为1024，期望数据被正常的写入到OTS中
- DONE 测试目的：测试限制项对writer的行为影响是否符合预期。测试内容：构造string的pk列，值长度为1025，期望该行数据被记录到脏数据回收器中
- DONE 测试目的：测试限制项对writer的行为影响是否符合预期。测试内容：构造binary的pk列，值长度为1024，期望数据被正常的写入到OTS中
- DONE 测试目的：测试限制项对writer的行为影响是否符合预期。测试内容：构造binary的pk列，值长度为1025，期望该行数据被记录到脏数据回收器中
- DONE 测试目的：测试限制项对writer的行为影响是否符合预期。测试内容：构造string的column列，值长度为64KB，期望数据被正常的写入到OTS中
- DONE 测试目的：测试限制项对writer的行为影响是否符合预期。测试内容：构造string的column列，值长度为64KB + 1，期望该行数据被记录到脏数据回收器中
- DONE 测试目的：测试限制项对writer的行为影响是否符合预期。测试内容：构造binary的column列，值长度为64PK，期望数据被正常的写入到OTS中
- DONE 测试目的：测试限制项对writer的行为影响是否符合预期。测试内容：构造binary的column列，值长度为64KB + 1，期望该行数据被记录到脏数据回收器中

## 多版本模式数据导入测试

### 多PK混合类型测试
- DONE 测试目的：测试在多PK和不同类型的组合下程序是否符合预期。测试内容，分别构造PK列为（1列PK）(string)、(integer)、(binary)、（2列PK）(string、integer)、（integer、binary）、（binary、string），（4列PK）（string，string，integer，binary）、（integer、string、binary，string）、（binary、string，integer、integer），期望数据符合预期。

### UpdateRow模式写入测试
- DONE 测试目的：测试在UpdateRow模式下，数据是否能正常的导入OTS中。测试内容：创建一个拥有1个PK的表，构造1行数据，该行数据包含128列，每列5个版本，导入OTS，期望数据符合预期
- DONE 测试目的：测试在UpdateRow模式下，数据是否能正常的导入OTS中。测试内容：创建一个拥有1个PK的表，构造10不重复行数据，该行数据包含12列，每列5个版本，导入OTS，期望数据符合预期
- DONE 测试目的：测试在UpdateRow模式下，数据是否能正常的导入OTS中。测试内容：创建一个拥有1个PK的表，构造50不重复行数据，该行数据包含2列，每列5个版本，导入OTS，期望数据符合预期
- DONE 测试目的：测试在UpdateRow模式下，数据是否能正常的导入OTS中。测试内容：创建一个拥有1个PK的表，构造100不重复行数据，该行数据包含2列，每列2个版本，导入OTS，期望数据符合预期
- DONE 测试目的：测试在UpdateRow模式下，数据是否能正常的导入OTS中。测试内容：创建一个拥有1个PK的表，构造500不重复行数据，该行数据包含2列，每列2个版本，导入OTS，期望数据符合预期
- DONE 测试目的：测试在UpdateRow模式下，数据是否能正常的导入OTS中。测试内容：创建一个拥有1个PK的表，构造10重复行数据，该行数据包含12列，每列5个版本，导入OTS，期望数据符合预期
- DONE 测试目的：测试在UpdateRow模式下，数据是否能正常的导入OTS中。测试内容：创建一个拥有1个PK的表，构造50重复行数据，该行数据包含2列，每列5个版本，导入OTS，期望数据符合预期
- DONE 测试目的：测试在UpdateRow模式下，数据是否能正常的导入OTS中。测试内容：创建一个拥有1个PK的表，构造100重复行数据，该行数据包含2列，每列2个版本，导入OTS，期望数据符合预期
- DONE 测试目的：测试在UpdateRow模式下，数据是否能正常的导入OTS中。测试内容：创建一个拥有1个PK的表，构造500重复行数据，该行数据包含2列，每列2个版本，导入OTS，期望数据符合预期

- DONE 测试目的：测试在UpdateRow模式下，数据是否能正常的导入OTS中。测试内容：创建一个拥有4个PK的表，构造1行数据，该行数据包含128列，每列5个版本，导入OTS，期望数据符合预期
- DONE 测试目的：测试在UpdateRow模式下，数据是否能正常的导入OTS中。测试内容：创建一个拥有4个PK的表，构造10不重复行数据，该行数据包含12列，每列5个版本，导入OTS，期望数据符合预期
- DONE 测试目的：测试在UpdateRow模式下，数据是否能正常的导入OTS中。测试内容：创建一个拥有4个PK的表，构造50不重复行数据，该行数据包含2列，每列5个版本，导入OTS，期望数据符合预期
- DONE 测试目的：测试在UpdateRow模式下，数据是否能正常的导入OTS中。测试内容：创建一个拥有4个PK的表，构造100不重复行数据，该行数据包含2列，每列2个版本，导入OTS，期望数据符合预期
- DONE 测试目的：测试在UpdateRow模式下，数据是否能正常的导入OTS中。测试内容：创建一个拥有4个PK的表，构造500不重复行数据，该行数据包含2列，每列2个版本，导入OTS，期望数据符合预期
- DONE 测试目的：测试在UpdateRow模式下，数据是否能正常的导入OTS中。测试内容：创建一个拥有4个PK的表，构造10重复行数据，该行数据包含12列，每列5个版本，导入OTS，期望数据符合预期
- DONE 测试目的：测试在UpdateRow模式下，数据是否能正常的导入OTS中。测试内容：创建一个拥有4个PK的表，构造50重复行数据，该行数据包含2列，每列5个版本，导入OTS，期望数据符合预期
- DONE 测试目的：测试在UpdateRow模式下，数据是否能正常的导入OTS中。测试内容：创建一个拥有4个PK的表，构造100重复行数据，该行数据包含2列，每列2个版本，导入OTS，期望数据符合预期
- DONE 测试目的：测试在UpdateRow模式下，数据是否能正常的导入OTS中。测试内容：创建一个拥有4个PK的表，构造500重复行数据，该行数据包含2列，每列2个版本，导入OTS，期望数据符合预期
- DONE 测试目的：测试在UpdateRow模式下，数据是否能正常的导入OTS中。测试内容：创建一个拥有1个PK的表，一行数据，该行依次包括构造1、129、128 * 100、128 * 100 + 1个Cell，导入OTS，期望数据符合预期

### 数据异常测试
- DONE 测试目的：测试datax传入不符合期望的数据，测试ots-writer的行为是否符合预期。测试内容：用户配置3列PK，构造10个Cell，其中一个Cell只传入2列PK，期望writer异常退出，错误消息符合预期
- DONE 测试目的：测试datax传入不符合期望的数据，测试ots-writer的行为是否符合预期。测试内容：用户配置3列PK，构造10个Cell，其中一个Cell只传入4列PK，期望writer异常退出，错误消息符合预期
- DONE 测试目的：测试datax传入不符合期望的数据，测试ots-writer的行为是否符合预期。测试内容：用户配置3列PK，构造10个Cell，其中一个Cell只传入3列PK，但是有一个PK列为空，期望该Cell被记录到脏数据回收器中，错误消息符合预期
- DONE 测试目的：测试datax传入不符合期望的数据，测试ots-writer的行为是否符合预期。测试内容：用户配置3列PK，构造10个Cell，其中一个Cell只传入3列PK，但是有一个PK列不能成功的转换为指定的类型，期望该Cell被记录到脏数据回收器中，错误消息符合预期
- DONE 测试目的：测试datax传入不符合期望的数据，测试ots-writer的行为是否符合预期。测试内容：构造10个Cell，其中一个Cell的columnName为空，期望该Cell被记录到脏数据回收器中，错误消息符合预期
- DONE 测试目的：测试datax传入不符合期望的数据，测试ots-writer的行为是否符合预期。测试内容：构造10个Cell，其中一个Cell的timestamp为空，期望该Cell被记录到脏数据回收器中，错误消息符合预期
- DONE 测试目的：测试datax传入不符合期望的数据，测试ots-writer的行为是否符合预期。测试内容：构造10个Cell，其中一个Cell的timestamp为不能转换为数字，期望该Cell被记录到脏数据回收器中，错误消息符合预期
- DONE 测试目的：测试datax传入不符合期望的数据，测试ots-writer的行为是否符合预期。测试内容：构造10个Cell，其中一个Cell的value为空，期望该Cell被记录到脏数据回收器中，错误消息符合预期
- DONE 测试目的：测试datax传入不符合期望的数据，测试ots-writer的行为是否符合预期。测试内容：构造10个Cell，其中一个Cell的value为不能转为指定的类型，期望该Cell被记录到脏数据回收器中，错误消息符合预期

### 限制项测试
- DONE 测试目的：测试限制项对writer的行为影响是否符合预期。测试内容：构造string的pk列，值得长度为1024，期望数据被正常的写入到OTS中
- DONE 测试目的：测试限制项对writer的行为影响是否符合预期。测试内容：构造string的pk列，值得长度为1025，期望该行数据被记录到脏数据回收器中
- DONE 测试目的：测试限制项对writer的行为影响是否符合预期。测试内容：构造binary的pk列，值得长度为1024，期望数据被正常的写入到OTS中
- DONE 测试目的：测试限制项对writer的行为影响是否符合预期。测试内容：构造binary的pk列，值得长度为1025，期望该行数据被记录到脏数据回收器中
- DONE 测试目的：测试限制项对writer的行为影响是否符合预期。测试内容：构造string的column列，值得长度为64KB，期望数据被正常的写入到OTS中
- DONE 测试目的：测试限制项对writer的行为影响是否符合预期。测试内容：构造string的column列，值得长度为64KB + 1，期望该行数据被记录到脏数据回收器中
- DONE 测试目的：测试限制项对writer的行为影响是否符合预期。测试内容：构造binary的column列，值得长度为64PK，期望数据被正常的写入到OTS中
- DONE 测试目的：测试限制项对writer的行为影响是否符合预期。测试内容：构造binary的column列，值得长度为64KB + 1，期望该行数据被记录到脏数据回收器中
