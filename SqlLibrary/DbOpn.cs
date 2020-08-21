using System;
using System.IO;
using System.Data.SqlClient;

namespace SqlLibrary
{
    public class DbOpn
    {

    #region 定数保持
        //ログディレクトリのパス保持
        const string _drivePath = @"C:/LOG";
        //ログテキストのパス保持
        const string _logTextName = @"C:/LOG/log.txt";
    #endregion

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
                                          "Connection Lifetime=2;"+
                                          "Connection Timeout=30;" ;//DBの接続待機時間を30秒に設定

                if (DB_Connection.State == System.Data.ConnectionState.Closed)
                {
                    //DB接続
                    DB_Connection.Open();
                }

                return true;
            }
            catch (Exception ex)
            {
                //エラーログ作成
                ErrorLogOutput(ex.ToString());

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
                    //DB切断
                    DB_Connection.Close();
                }

                DB_Connection.Dispose();
                DB_Connection = null;

                return true;

            }
            catch (Exception ex)
            {
                //エラーログ作成
                ErrorLogOutput(ex.ToString());

                return false;
            }
        }

        // SQL実行
        public bool DB_SqlReader(string strSql, ref SqlDataReader sqlRdr)
        {
            SqlCommand sqlCmn = new SqlCommand();
            sqlCmn.CommandTimeout = 60;//SQLの実行待機時間を60秒に設定

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
                //エラーログ作成
                ErrorLogOutput(ex.ToString());

                return false;
            }
        }
        
        public string GET_headLine(string strSql)
        {
            SqlDataReader sqlRdr = null;
            string strHeadLine = "";


            System.Diagnostics.Stopwatch stopwatch = new System.Diagnostics.Stopwatch();
            stopwatch.Start();

            try
            {
                if (!DB_Connect()　|| stopwatch.ElapsedMilliseconds > 3000)//DB接続
                {
                    return "";
                }

                if (!DB_SqlReader(strSql, ref sqlRdr))//データ取得
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
                stopwatch.Stop();
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

        /// <summary>
        /// エラーログ出力処理
        /// </summary>
        /// <param name="errormsg"></param>
        public void ErrorLogOutput(string errormsg) 
        {
            //指定したパスにディレクトリがあるかどうか判断。ない場合のみ新規作成
            SafeCreateDirectory(_drivePath);

            //エラー内容を指定パスファイルに上書き。ない場合新規作成。
            File.AppendAllText(_logTextName, errormsg);

        }

        public static void SafeCreateDirectory(string path)
        {
            //パスにない場合はディレクトリ新規作成
            if (!Directory.Exists(path))
            {
                Directory.CreateDirectory(path);
            }
        }
    }
}
