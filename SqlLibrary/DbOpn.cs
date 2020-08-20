using System;
using System.IO;
using System.Text;
using System.Data.SqlClient;

namespace SqlLibrary
{
    public class DbOpn
    {
        // SQL Server用コネクション
        public SqlConnection DB_Connection;
        // DB接続
        public bool DB_Connect()
        {
            DB_Connection = new SqlConnection();

            try
            {
                // 接続文字列を作成して接続を開始する
                DB_Connection.ConnectionString = "data source = " + CONSTOpn.cstrDB_Sorce + ";" +
                                          "initial catalog = " + CONSTOpn.cstrDB_Name + ";" +
                                          "user id         = " + CONSTOpn.cstrDB_User + ";" +
                                          "password        = " + CONSTOpn.cstrDB_Pass + ";" +
                                          "persist security info=True;" +
                                          "Pooling=True;" +
                                          "Min Pool Size=20;" +
                                          "Max Pool Size=200;" +
                                          "Connection Lifetime=2;";

                if (DB_Connection.State == System.Data.ConnectionState.Closed)
                {
                    DB_Connection.Open();
                }

                return true;
            }
            catch (Exception ex)
            {
                if (DB_Connection != null)
                {
                    DB_Connection.Close();
                }

                return false;
            }
        }

        // DB切断
        public bool DB_Close()
        {
            try
            {
                if (DB_Connection.State == System.Data.ConnectionState.Open)
                {
                    DB_Connection.Close();
                }

                DB_Connection.Dispose();
                DB_Connection = null;

                return true;

            }
            catch (Exception ex)
            {
                return false;
            }
        }

        // SQL実行
        public bool DB_SqlReader(string strSql, ref SqlDataReader sqlRdr)
        {
            SqlCommand sqlCmn = new SqlCommand();

            try
            {
                sqlCmn = new SqlCommand(strSql, DB_Connection);
                sqlCmn.Connection = DB_Connection;
                sqlRdr = sqlCmn.ExecuteReader();
                sqlCmn.Dispose();

                return true;
            }
            catch (Exception ex)
            {
                return false;
            }
        }



        public string GET_headLine(string strSql)
        {
            SqlDataReader sqlRdr = null;
            
            string strHeadLine = "";

            try
            {
                if (!DB_Connect())//DB接続
                {
                    return "";
                }

                if (!DB_SqlReader(strSql., ref sqlRdr))//データ取得
                {
                    return "";
                }

                return strHeadLine;
            }
            catch (Exception ex)
            {
                return "";
            }
            finally
            {
                if (sqlRdr != null)
                {
                    if (!sqlRdr.IsClosed)
                    {
                        sqlRdr.Close();
                    }
                }
                DB_Close();//DB切断
            }
        }

    }
}
