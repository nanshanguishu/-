using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace 大数据量批量更新
{
    class Program
    {
        static void Main(string[] args)
        {
        }
        /// <summary>
        /// 创建临时表存储当前需要提交的数据
        /// </summary>
        /// <param name="databaseConnectionString">数据库连接字符串</param>
        /// <param name="tableName">当前更新导入的表名</param>
        /// <returns></returns>
        public static string CreateTempTable(string databaseConnectionString, string tableName)
        {
            string tempTableName = string.Empty;
            string sql = string.Empty;
            //创建临时表存储当前需要提交的数据
            for (int i = 0; i < 20; i++)  //只是设定一个临时表的序号，如果是多台主机导入同一个数据库的时候，在数据库中可能会创建同名的临时表
            {
                sql = string.Format("SELECT * INTO {0} FROM {1} where 1=2", tableName + "_TEMP" + i, tableName);
                try
                {
                    ExecuteQuery(sql, databaseConnectionString);
                    tempTableName = tableName + "_TEMP" + i;
                    break;
                }
                catch //如果在创建当前的临时表的过程中发生了错误，如当前临时表已经存在或者被占用，则继续通过 序号i去创建下一张临时表，直到创建成功为止
                {
                    continue;
                }
            }
            return tempTableName;
        }
        /// <summary>
        /// 数据批量插入方法
        /// </summary>
        /// <param name="connectionString">目标连接字符</param>
        /// <param name="TableName">目标表</param>
        /// <param name="dt">源数据</param>
        public static void SqlBulkCopyByDatatable(string connectionString, string TableName, DataTable dt)
        {
            using (var conn = new SqlConnection(connectionString))
            using (var sqlbulkcopy = new SqlBulkCopy(connectionString, SqlBulkCopyOptions.UseInternalTransaction))
            {
                try
                {
                    sqlbulkcopy.BulkCopyTimeout = 600;
                    sqlbulkcopy.DestinationTableName = TableName;
                    for (int i = 0; i < dt.Columns.Count; i++)
                    {
                        sqlbulkcopy.ColumnMappings.Add(dt.Columns[i].ColumnName, dt.Columns[i].ColumnName);
                    }
                    sqlbulkcopy.WriteToServer(dt);
                }
                catch (System.Exception ex)
                {
                    throw ex;
                }
            }
        }
        /// <summary>
        /// 将临时表和当前需要更新的表的数据进行对比，根据表内的唯一标识提取出在当前表中不存在的数据，然后批量插入新增的数据
        /// </summary>
        /// <param name="databaseConnectionString">数据库连接字符串</param>
        /// /// <param name="dt">更新需要提交数据的datatable</param>
        /// <param name="tableName">当前更新导入的表名</param>
        /// <param name="tempTableName">临时表表名</param>
        /// <returns></returns>
        public static string GetUpdateData(string databaseConnectionString, DataTable dt, string tableName, string tempTableName)
        {
            string errMess = string.Empty;
            try
            {
                string sql = string.Empty;
                string found = string.Empty;
                List<string> keyList = new List<string>();
                //提取需要批量插入的数据的唯一标识列表
                sql = string.Format("select key from {0}  except select key from {1}", tempTableName, tableName);
                found = string.Format("key= '{0}'", "@key");  //生成在当前的datatable查找的语句，key为查找的关键字，当前使用@key来代替所需的关键字
                #region 批量插入数据库中不存在的数据

                keyList = new List<string>();  //用于存储当前查询的唯一标识中在临时表中存在而在数据库表中不存在的数据列表
                keyList = GetSqlAsString(sql, null, databaseConnectionString);
                DataTable appendData = dt.Clone();
                for (int i = 0; i < keyList.Count; i++)
                {
                    DataRow[] dr = dt.Select(found.Replace("@key", keyList[i]));
                    foreach (var row in dr)
                    {
                        appendData.ImportRow(row);
                    }
                }
                SqlBulkCopyByDatatable(databaseConnectionString, tableName, appendData);
                #endregion
                return errMess;
            }
            catch (Exception ex)
            {
                errMess = ex.Message;
                return errMess;
            }
        }
        /// <summary>
        /// 以字符串列表的方式返回查询的结果集
        /// </summary>
        /// <param name="sqlText">查询语句</param>
        /// <param name="sqlParameters">事物</param>
        /// <param name="databaseConnectionString">数据库连接字符串</param>
        /// <returns></returns>
        static public List<string> GetSqlAsString(string sqlText, SqlParameter[] sqlParameters, string databaseConnectionString)
        {
            List<string> result = new List<string>();
            SqlDataReader reader;
            SqlConnection connection = new SqlConnection(databaseConnectionString);
            using (connection)
            {
                SqlCommand sqlcommand = connection.CreateCommand();
                sqlcommand.CommandText = sqlText;
                if (sqlParameters != null)
                {
                    sqlcommand.Parameters.AddRange(sqlParameters);
                }
                connection.Open();
                reader = sqlcommand.ExecuteReader();
                if (reader != null)
                {
                    while (reader.Read())
                    {
                        var re = reader.GetString(0);
                        if (!string.IsNullOrEmpty(re))
                            result.Add(re);
                    }
                }
            }
            return result;
        }

        /// <summary>
        /// 根据临时表更新当前导入表的数据
        /// </summary>
        /// <param name="databaseConnectionString">数据库连接字符串</param>
        /// <param name="tableName">当前更新导入的表名</param>
        /// <param name="tempTableName">临时表表名</param>
        /// <returns></returns>
        public static string UpdateTableData(string databaseConnectionString, string tableName, string tempTableName)
        {
            string errMess = string.Empty;
            try
            {
                var filedList = GetFileds(databaseConnectionString, tableName);  //获取当前操作表的字段信息
                StringBuilder sql = new StringBuilder();  //构造更新当前表的所有字段的数据信息的更新语句
                sql.Append(string.Format("update {0} set ", tableName));
                foreach (var filed in filedList)
                {
                    sql.Append(string.Format("{0} = {1}.{2},", filed, tempTableName, filed));
                }
                sql.Append("@");
                sql.Replace(",@", " ");
                sql.Append(string.Format("from {0} where tableName.key= {1}.key", tempTableName, tempTableName));  //利用唯一标识更新数据
                ExecuteQuery(sql.ToString(), databaseConnectionString);
                return errMess;
            }
            catch (Exception ex)
            {
                errMess = ex.Message;
                return errMess;
            }
        }
        /// <summary>
        /// 删除临时表
        /// </summary>
        /// <param name="databaseConnectionString">数据库连接字符串</param>
        /// <param name="tempTableName">需要删除的临时表表名</param>
        /// <returns></returns>
        public static string DropTempTable(string databaseConnectionString, string tempTableName)
        {
            string errMess = string.Empty;
            try
            {
                string sql = string.Empty;
                sql = string.Format("drop table {0}", tempTableName);
                ExecuteQuery(sql, databaseConnectionString);
                return errMess;
            }
            catch (Exception ex)
            {
                errMess = ex.Message;
                return errMess;
            }
        }
        /// <summary>
        /// 执行查询结果
        /// </summary>
        /// <param name="sql">执行语句</param>
        /// <param name="databaseConnectionString">数据库连接字符串</param>
        public static void ExecuteQuery(string sql, string databaseConnectionString)
        {
            SqlConnection conn = new SqlConnection(databaseConnectionString);
            SqlCommand cmd = new SqlCommand(sql, conn);
            conn.Open();
            cmd.ExecuteNonQuery();
            conn.Close();
        }
        /// <summary>
        /// 获取当前操作表的所有字段名
        /// </summary>
        /// <param name="connectionString">数据库连接字符串</param>
        /// <param name="tableName">当前操作的表名</param>
        /// <returns></returns>
        private static List<string> GetFileds(string databaseConnectionString, string tableName)
        {
            List<string> _Fields = new List<string>();
            SqlConnection _Connection = new SqlConnection(databaseConnectionString);
            try
            {
                _Connection.Open();
                string key = GetTableKey(databaseConnectionString, tableName);
                string[] restrictionValues = new string[4];
                restrictionValues[0] = null; // Catalog
                restrictionValues[1] = null; // Owner
                restrictionValues[2] = tableName; // Table
                restrictionValues[3] = null; // Column

                using (DataTable dt = _Connection.GetSchema(SqlClientMetaDataCollectionNames.Columns, restrictionValues))
                {
                    foreach (DataRow dr in dt.Rows)
                    {
                        var filedName = dr["column_name"].ToString();
                        if (!filedName.Equals(key))  //不将当前表的主键添加进去
                            _Fields.Add(filedName);
                    }
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                _Connection.Dispose();
            }
            return _Fields;
        }

        /// <summary>
        /// 根据表名获取主键字段
        /// </summary>
        /// <param name="databaseConnectionString">数据库连接字符串</param>
        /// <param name="TableName">表名</param>
        /// <returns>主键字段</returns>
        public static string GetTableKey(string databaseConnectionString, string TableName)
        {
            try
            {
                if (TableName == "")
                    return null;
                StringBuilder sb = new StringBuilder();
                sb.Append("SELECT COLUMN_NAME ");
                sb.Append("FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE ");
                sb.Append("WHERE TABLE_NAME=");
                sb.Append("'" + TableName + "'");
                string key = string.Empty;
                var keyList = GetSqlAsString(sb.ToString(), null, databaseConnectionString);
                if (keyList.Count > 0)
                    key = keyList[0];
                return key;
            }
            catch
            {
                return null;
            }
        }
    }
}
