package com.fansin.mybatis;

import com.alibaba.fastjson.JSONObject;
import org.beetl.core.Configuration;
import org.beetl.core.GroupTemplate;
import org.beetl.core.Template;
import org.beetl.core.resource.FileResourceLoader;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by zhaofeng on 16-5-12.
 * Desc:
 */
public class MybatisFileGenerator {
    //是否可以删除文件,慎重！
    private static boolean ACCESS_DELETED = true;

    public static void main(String[] args) {

        String url = "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS = (PROTOCOL = TCP)(HOST = 172.20.20.142) (PORT = 1521))(LOAD_BALANCE=yes)(FAILOVER = ON)(CONNECT_DATA = (SERVICE_NAME = dbopsmt)))";
        String user = "auth_user";
        String passwd = "111111";
//        String url = "jdbc:mysql://172.20.0.193:3308/beetlsql?characterEncoding=utf8&useSSL=true";
//        String user = "root";
//        String passwd = "dev001";
        /*表名(mysql默认区分大小写)+空格+类名*/
        String[] tables = {"TRADEELEMENT TradeElement"/*, "ACQUIRER Acquirer","user User"*/};

        String packagePath = "com.fansin.mybatis";
        String tempDir = "src/test/resources/templates/";
        execute(url, user, passwd, tables, packagePath, tempDir);

    }


    private static void getTableMeta(String url, String user, String passwd, String[] tables, String packagePath, String tempDir) {

        //分割表名和java文件名
        //拼接表名串
        StringBuilder tableStrs = new StringBuilder("(");
        //java文件名
        Map<String, TableProperties> tableMap = new HashMap<String, TableProperties>();
        String[] tempArr;
        for (String tab : tables) {
            tempArr = tab.split(" ");
            tableStrs.append("'").append(tempArr[0]).append("',");
            tableMap.put(tempArr[1], new TableProperties(tempArr[0].toUpperCase(), packagePath, tempArr[1]));
        }
        tableStrs.deleteCharAt(tableStrs.lastIndexOf(","));
        tableStrs.append(")");

        //获取表名，字段，类型，注释
        String sql = "SELECT DECODE (tab.column_id, 1, 1, 0) AS is_pk, tab.table_name AS table_name, tab.column_name AS column_name, tab.data_type AS data_type,col.COMMENTS AS column_comment FROM all_tab_columns tab, all_col_comments col WHERE tab.table_name = col.table_name AND tab.column_name = col.column_name AND tab.table_name IN " + tableStrs.toString();

        String driver_class = "oracle.jdbc.driver.OracleDriver";
        if (url.contains("mysql")) {
            driver_class = "com.mysql.jdbc.Driver";
            int begIndex = url.lastIndexOf("/") + 1;
            int endIndex = url.lastIndexOf("?") == -1 ? url.length() : url.lastIndexOf("?");
            String schema = url.substring(begIndex, endIndex);
            sql = "SELECT table_name,column_name,column_comment,data_type,if(column_key='PRI',1,0) as is_pk FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = '" + schema + "' AND table_name in " + tableStrs.toString();
        }
        System.out.println("SQL:\n" + sql);
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            Class.forName(driver_class);
            conn = DriverManager.getConnection(url, user, passwd);
            ps = conn.prepareStatement(sql);
            rs = ps.executeQuery();

            while (rs.next()) {
                String tableName = rs.getString("table_name").toLowerCase();
                String columnName = rs.getString("column_name").toLowerCase();
                String dataType = rs.getString("data_type").toUpperCase();
                String comments = rs.getString("column_comment");
                boolean is_pk = rs.getBoolean("is_pk");
                for (Map.Entry<String, TableProperties> entry : tableMap.entrySet()
                        ) {
                    String key = entry.getKey();
                    if (key.equalsIgnoreCase(tableName)) {
                        JavaFileColumn column = new JavaFileColumn();
                        column.setComment(comments);
                        column.setName(columnName);
                        column.setSetName(captureName(columnName));
                        column.setDataType(dataType.toUpperCase());
                        column.setPk(is_pk);
                        entry.getValue().getColumnList().add(column);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {

                if (rs != null) {
                    rs.close();
                }
                if (ps != null) {
                    ps.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }


        //生成java文件属性
        System.out.println(JSONObject.toJSONString(tableMap));
        //sql相关属性
        System.out.println("----------开始生成文件------------");
        try {
            createFile(tableMap, tempDir);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("----------文件生成成功------------");


    }

    private static void createFile(Map<String, TableProperties> tableMap, String tempDir) throws IOException {
        //模板目录
        String templateDir = System.getProperty("user.dir") + File.separator + tempDir;

        //循环表信息
        for (Map.Entry<String, TableProperties> entry : tableMap.entrySet()
                ) {
            String tableName = entry.getKey();
            TableProperties tableProp = entry.getValue();

            //循环模板信息并生成文件
            recursionGenerator(new File(templateDir), tableName, tableProp);
        }

    }

    /**
     * 递归遍历模板文件，生成java文件
     *
     * @param dirFile
     * @param tableName
     * @param tableProp
     */
    private static void recursionGenerator(File dirFile, String tableName, TableProperties tableProp) throws IOException {
        if (!dirFile.exists()) {
            throw new RuntimeException("文件不存在！ " + dirFile.getAbsolutePath());
        }

        if (dirFile.isDirectory()) {
            File[] files = dirFile.listFiles();
            if (files != null) {
                for (File file : files) {
                    recursionGenerator(file, tableName, tableProp);
                }
            }
        } else if (dirFile.getName().endsWith(".btl")) {
            String realDir = dirFile.getParentFile().getPath();
            FileResourceLoader resourceLoader = new FileResourceLoader("/");
            Configuration cfg = Configuration.defaultConfiguration();
            GroupTemplate gt = new GroupTemplate(resourceLoader, cfg);
            Template t = gt.getTemplate(dirFile.getPath());
            t.binding("table", tableProp);
            t.binding("columnList", tableProp.getColumnList());

            //生成到输出目录
//            realDir = realDir.replace("resources/templates", "resources/out");
            String packagePath = tableProp.getPackagePath();
            realDir = realDir.replace("test/resources/templates/java", "main/java/" + packagePath.replace(".", File.separator));
            //默认文件
            String fileName = tableName + ".java";
            if (dirFile.getName().endsWith("Mapper.btl")) {
                fileName = tableName + "Mapper.java";
            } else if (dirFile.getName().endsWith("Service.btl")) {
                fileName = tableName + "Service.java";
            } else if (dirFile.getName().endsWith("ServiceImpl.btl")) {
                fileName = tableName + "ServiceImpl.java";
            } else if (dirFile.getName().endsWith("XML.btl")) {
                fileName = tableName + "Mapper.xml";
                realDir = realDir.replace("test/resources/templates/xml/", "main/resources/");
            }
            File javaFile = new File(realDir + File.separator + fileName);

            File javaDir = javaFile.getParentFile();
            if (!javaDir.exists()) {
                boolean result = javaDir.mkdirs();
                System.out.println("文件夹不存在,创建结果：" + result + " " + javaDir.getPath());
            }
            if (javaFile.exists() && !ACCESS_DELETED) {
                throw new RuntimeException("文件已存在！" + javaFile.getAbsolutePath());
            }

            OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(javaFile), "UTF-8");
            t.renderTo(writer);
//            writer.flush();
            writer.close();
            System.out.println("生成文件：" + javaFile.getName() + " " + javaFile.getAbsolutePath());
        }


    }

    private static void execute(String url, String user, String passwd, String[] tables, String packagePath, String tempDir) {
        getTableMeta(url, user, passwd, tables, packagePath, tempDir);
    }

    /**
     * 首字母大写
     *
     * @param str
     * @return
     */
    private static String captureName(String str) {
        char[] chars = str.toCharArray();
        chars[0] -= 32;
        return String.valueOf(chars);
    }


    private static class TableProperties {
        private String tableName;
        private String packagePath;
        private String className;
        private List<JavaFileColumn> columnList = new ArrayList<JavaFileColumn>();

        public TableProperties(String tableName, String packagePath, String className) {
            this.tableName = tableName;
            this.packagePath = packagePath;
            this.className = className;
        }

        public String getPackagePath() {
            return packagePath;
        }

        public void setPackagePath(String packagePath) {
            this.packagePath = packagePath;
        }

        public String getClassName() {
            return className;
        }

        public void setClassName(String className) {
            this.className = className;
        }

        public List<JavaFileColumn> getColumnList() {
            return columnList;
        }

        public void setColumnList(List<JavaFileColumn> columnList) {
            this.columnList = columnList;
        }

        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }
    }

    private static class JavaFileColumn {
        private String name;
        private String setName;
        private String comment;
        private String dataType;
        private boolean pk;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getSetName() {
            return setName;
        }

        public void setSetName(String setName) {
            this.setName = setName;
        }

        public String getComment() {
            return comment;
        }

        public void setComment(String comment) {
            this.comment = comment;
        }

        public String getDataType() {
            return dataType;
        }

        public void setDataType(String dataType) {
            this.dataType = dataType;
        }

        public boolean getPk() {
            return pk;
        }

        public void setPk(boolean pk) {
            this.pk = pk;
        }
    }
}
