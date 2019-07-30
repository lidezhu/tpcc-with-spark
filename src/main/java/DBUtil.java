import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;


public class DBUtil implements TpccConstants {

    private static final Logger logger = LoggerFactory.getLogger(DBUtil.class);
    private static final String URL="jdbc:mysql://172.16.5.81:12490/tpcc";
    private static final String NAME="root";
    private static final String PASSWORD="";

    private static SparkSession spark;

    private static final String PD_ADDRESSES = "spark.tispark.pd.addresses";

    private static final String FLASH_ADDRESSES = "spark.flash.addresses";

    private static final String PARTITION_PER_SPLIT = "spark.storage.partitionsPerSplit";

    private static final String DEFAULT_PARTITION_PER_SPLIT = "1";

    private static final int MAX_RETRY = 10;
    private static boolean DEBUG = true;

    private static String[] iname = new String[MAX_NUM_ITEMS];
    private static String[] bg = new String[MAX_NUM_ITEMS];
    private static float[] amt = new float[MAX_NUM_ITEMS];
    private static float[] price = new float[MAX_NUM_ITEMS];
    private static int[] stock = new int[MAX_NUM_ITEMS];
    private static int[] ol_num_seq = new int[MAX_NUM_ITEMS];

    private static String s_dist_01 = null;
    private static String s_dist_02 = null;
    private static String s_dist_03 = null;
    private static String s_dist_04 = null;
    private static String s_dist_05 = null;
    private static String s_dist_06 = null;
    private static String s_dist_07 = null;
    private static String s_dist_08 = null;
    private static String s_dist_09 = null;
    private static String s_dist_10 = null;

    private static TpccStatements pStmts;

    // configurable parameter
    public static int num_ware = 10;

    private static String pickDistInfo(String ol_dist_info, int ol_supply_w_id) {
        switch (ol_supply_w_id) {
            case 1:
                ol_dist_info = s_dist_01;
                break;
            case 2:
                ol_dist_info = s_dist_02;
                break;
            case 3:
                ol_dist_info = s_dist_03;
                break;
            case 4:
                ol_dist_info = s_dist_04;
                break;
            case 5:
                ol_dist_info = s_dist_05;
                break;
            case 6:
                ol_dist_info = s_dist_06;
                break;
            case 7:
                ol_dist_info = s_dist_07;
                break;
            case 8:
                ol_dist_info = s_dist_08;
                break;
            case 9:
                ol_dist_info = s_dist_09;
                break;
            case 10:
                ol_dist_info = s_dist_10;
                break;
        }

        return ol_dist_info;
    }

    public static boolean checkSparkRead(String sql, String fields[], String result[]) {
        Dataset<Row> resultSet = spark.sql(sql);
        boolean matched = true;
        for (int i = 0; i < fields.length; i++) {
            String value = resultSet.first().getAs(fields[i]).toString();
            if (!value.equals(result[i])) {
                matched = false;
                System.out.println(sql);
                System.out.println("field: " + fields[i]);
                System.out.println("fetch values: " + value);
                System.out.println("expected values: " + result[i]);
            }
        }
        return matched;
    }

    public static int newOrder(int w_id_arg,        /* warehouse id */
                      int d_id_arg,        /* district id */
                      int c_id_arg,        /* customer id */
                      int o_ol_cnt_arg,            /* number of items */
                      int o_all_local_arg,    /* are all order lines local */
                      int itemid[],        /* ids of items to be ordered */
                      int supware[],        /* warehouses supplying items */
                      int qty[]
    ) throws SQLException {

        try {
            // Start a transaction.
            pStmts.setAutoCommit(false);
            if (DEBUG) logger.debug("Transaction:	New Order");
            int w_id = w_id_arg;
            int d_id = d_id_arg;
            int c_id = c_id_arg;
            int o_ol_cnt = o_ol_cnt_arg;
            int o_all_local = o_all_local_arg;
            float c_discount = 0;
            String c_last = null;
            String c_credit = null;
            float w_tax = 0;
            int d_next_o_id = 0;
            float d_tax = 0;
            int o_id = 0;
            String i_name = null;
            float i_price = 0;
            String i_data = null;
            int ol_i_id = 0;
            int s_quantity = 0;
            String s_data = null;

            String ol_dist_info = null;
            int ol_supply_w_id = 0;
            float ol_amount = 0;
            int ol_number = 0;
            int ol_quantity = 0;

            Map<String, ArrayList<String>> sqlToFields = new HashMap<String, ArrayList<String>>();
            Map<String, ArrayList<String>> sqlToResults = new HashMap<String, ArrayList<String>>();


            int min_num = 0;
            int i = 0, j = 0, tmp = 0, swp = 0;

            //Timestamp
            java.sql.Timestamp time = new Timestamp(System.currentTimeMillis());
            //String currentTimeStamp = "'" + time.toString() + "'";
            String currentTimeStamp = time.toString();


            //Get prepared statement
            //"SELECT c_discount, c_last, c_credit, w_tax FROM customer, warehouse
            // WHERE w_id = ? AND c_w_id = w_id AND c_d_id = ? AND c_id = ?"
            try {
                int column = 1;
                final PreparedStatement pstmt0 = pStmts.getStatement(0);
                pstmt0.setInt(column++, w_id);
                pstmt0.setInt(column++, w_id);
                pstmt0.setInt(column++, d_id);
                pstmt0.setInt(column++, c_id);
                ResultSet rs = pstmt0.executeQuery();

                if (rs.next()) {
                    c_discount = rs.getFloat(1);
                    c_last = rs.getString(2);
                    c_credit = rs.getString(3);
                    w_tax = rs.getFloat(4);
                }
            } catch (SQLException e) {
                logger.error("SELECT c_discount, c_last, c_credit, w_tax FROM customer, warehouse WHERE w_id = " + w_id + " AND c_w_id = " + w_id + " AND c_d_id = " + d_id + " AND c_id = " + c_id, e);
                throw new Exception("NewOrder select transaction error", e);
            }

            //Get prepared statement
            //"SELECT d_next_o_id, d_tax FROM district WHERE d_id = ? AND d_w_id = ? FOR UPDATE"
            try {
                final PreparedStatement pstmt1 = pStmts.getStatement(1);
                pstmt1.setInt(1, d_id);
                pstmt1.setInt(2, w_id);
                ResultSet rs = pstmt1.executeQuery();
                if (rs.next()) {
                    d_next_o_id = rs.getInt(1);
                    d_tax = rs.getFloat(2);
                } else {
                    logger.error("Failed to obtain d_next_o_id. No results to query: "
                            + "SELECT d_next_o_id, d_tax FROM district WHERE d_id = " + d_id + "  AND d_w_id = " + w_id + " FOR UPDATE");
                }
            } catch (SQLException e) {
                logger.error("SELECT d_next_o_id, d_tax FROM district WHERE d_id = " + d_id + "  AND d_w_id = " + w_id + " FOR UPDATE", e);
                throw new Exception("Neworder select transaction error", e);
            }

            //Get prepared statement
            //"UPDATE district SET d_next_o_id = ? + 1 WHERE d_id = ? AND d_w_id = ?"
            try {
                final PreparedStatement pstmt2 = pStmts.getStatement(2);
                pstmt2.setInt(1, d_next_o_id);
                pstmt2.setInt(2, d_id);
                pstmt2.setInt(3, w_id);
                pstmt2.executeUpdate();
            } catch (SQLException e) {
                logger.error("UPDATE district SET d_next_o_id = " + d_next_o_id + " + 1 WHERE d_id = " + d_id + " AND d_w_id = " + w_id, e);
                throw new Exception("NewOrder update transaction error", e);
            }

            ArrayList<String> fields = new ArrayList<String>();
            ArrayList<String> results = new ArrayList<String>();
            fields.add("d_next_o_id");
            fields.add("d_id");
            fields.add("d_w_id");
            results.add(Integer.toString(d_next_o_id + 1));
            results.add(Integer.toString(d_id));
            results.add(Integer.toString(w_id));
            String sql = "select * from tpcc.bmsql_district where d_id='" + d_id + "'and d_w_id='" + w_id +"'";
            sqlToFields.put(sql, (ArrayList<String>)fields.clone());
            sqlToResults.put(sql, (ArrayList<String>)results.clone());

            o_id = d_next_o_id;


            //Get prepared statement
            //"INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local) VALUES(?, ?, ?, ?, ?, ?, ?)"

            try {
                final PreparedStatement pstmt3 = pStmts.getStatement(3);
                pstmt3.setInt(1, o_id);
                pstmt3.setInt(2, d_id);
                pstmt3.setInt(3, w_id);
                pstmt3.setInt(4, c_id);
                pstmt3.setString(5, currentTimeStamp);
                pstmt3.setInt(6, o_ol_cnt);
                pstmt3.setInt(7, o_all_local);
                pstmt3.executeUpdate();
            } catch (SQLException e) {
                logger.error("INSERT INTO bmsql_oorder (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local) " +
                        "VALUES(" + o_id + "," + d_id + "," + w_id + "," + c_id + "," + currentTimeStamp + "," + o_ol_cnt + "," + o_all_local + ")", e);
                throw new Exception("NewOrder insert transaction error", e);
            }

            fields.clear();
            results.clear();
            fields.add("o_id");
            fields.add("o_d_id");
            fields.add("o_w_id");
            fields.add("o_c_id");
            fields.add("o_ol_cnt");
            fields.add("o_all_local");
            results.add(Integer.toString(o_id));
            results.add(Integer.toString(d_id));
            results.add(Integer.toString(w_id));
            results.add(Integer.toString(c_id));
            results.add(Integer.toString(o_ol_cnt));
            results.add(Integer.toString(o_all_local));

            sql = "select * from tpcc.bmsql_oorder where o_w_id='" + w_id + "'and o_d_id='" + d_id +"' and o_id='" + o_id + "'";
            sqlToFields.put(sql, (ArrayList<String>)fields.clone());
            sqlToResults.put(sql, (ArrayList<String>)results.clone());

            //Get prepared statement
            //"INSERT INTO new_orders (no_o_id, no_d_id, no_w_id) VALUES (?,?,?)
            try {
                final PreparedStatement pstmt4 = pStmts.getStatement(4);
                pstmt4.setInt(1, o_id);
                pstmt4.setInt(2, d_id);
                pstmt4.setInt(3, w_id);
                pstmt4.executeUpdate();
            } catch (SQLException e) {
                logger.error("INSERT INTO bmsql_new_order (no_o_id, no_d_id, no_w_id) VALUES (" + o_id + "," + d_id + "," + w_id + ")", e);
                throw new Exception("NewOrder insert transaction error", e);
            }

            fields.clear();
            results.clear();
            fields.add("no_o_id");
            fields.add("no_d_id");
            fields.add("no_w_id");
            results.add(Integer.toString(o_id));
            results.add(Integer.toString(d_id));
            results.add(Integer.toString(w_id));

            sql = "select * from tpcc.bmsql_new_order where no_w_id='" + w_id + "'and no_d_id='" + d_id +"' and no_o_id='" + o_id + "'";
            sqlToFields.put(sql, (ArrayList<String>)fields.clone());
            sqlToResults.put(sql, (ArrayList<String>)results.clone());

            /* sort orders to avoid DeadLock */
            for (i = 0; i < o_ol_cnt; i++) {
                ol_num_seq[i] = i;
            }

            for (i = 0; i < (o_ol_cnt - 1); i++) {
                tmp = (MAXITEMS + 1) * supware[ol_num_seq[i]] + itemid[ol_num_seq[i]];
                min_num = i;
                for (j = i + 1; j < o_ol_cnt; j++) {
                    if ((MAXITEMS + 1) * supware[ol_num_seq[j]] + itemid[ol_num_seq[j]] < tmp) {
                        tmp = (MAXITEMS + 1) * supware[ol_num_seq[j]] + itemid[ol_num_seq[j]];
                        min_num = j;
                    }
                }
                if (min_num != i) {
                    swp = ol_num_seq[min_num];
                    ol_num_seq[min_num] = ol_num_seq[i];
                    ol_num_seq[i] = swp;
                }
            }

            for (ol_number = 1; ol_number <= o_ol_cnt; ol_number++) {
                ol_supply_w_id = supware[ol_num_seq[ol_number - 1]];
                ol_i_id = itemid[ol_num_seq[ol_number - 1]];
                ol_quantity = qty[ol_num_seq[ol_number - 1]];

                /* EXEC SQL WHENEVER NOT FOUND GOTO invaliditem; */
                //Get prepared statement
                //"SELECT i_price, i_name, i_data FROM item WHERE i_id = ?"

                try {
                    final PreparedStatement pstmt5 = pStmts.getStatement(5);
                    pstmt5.setInt(1, ol_i_id);
                    ResultSet rs = pstmt5.executeQuery();
                    if (rs.next()) {
                        i_price = rs.getFloat(1);
                        i_name = rs.getString(2);
                        i_data = rs.getString(3);
                    } else {
                        if (DEBUG) {
                            logger.debug("No item found for item id " + ol_i_id);
                        }
                        throw new AbortedTransactionException();
                    }
                } catch (SQLException e) {
                    logger.error("SELECT i_price, i_name, i_data FROM item WHERE i_id =" + ol_i_id, e);
                    throw new Exception("NewOrder select transaction error", e);
                }

                price[ol_num_seq[ol_number - 1]] = i_price;
                iname[ol_num_seq[ol_number - 1]] = i_name;

                //Get prepared statement
                //"SELECT s_quantity, s_data, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10 FROM stock WHERE s_i_id = ? AND s_w_id = ? FOR UPDATE"
                try {
                    final PreparedStatement pstmt6 = pStmts.getStatement(6);
                    pstmt6.setInt(1, ol_i_id);
                    pstmt6.setInt(2, ol_supply_w_id);

                    ResultSet rs = pstmt6.executeQuery();
                    if (rs.next()) {
                        s_quantity = rs.getInt(1);
                        s_data = rs.getString(2);
                        s_dist_01 = rs.getString(3);
                        s_dist_02 = rs.getString(4);
                        s_dist_03 = rs.getString(5);
                        s_dist_04 = rs.getString(6);
                        s_dist_05 = rs.getString(7);
                        s_dist_06 = rs.getString(8);
                        s_dist_07 = rs.getString(9);
                        s_dist_08 = rs.getString(10);
                        s_dist_09 = rs.getString(11);
                        s_dist_10 = rs.getString(12);
                    }

                } catch (SQLException e) {
                    logger.error("SELECT s_quantity, s_data, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10 FROM " +
                            "stock WHERE s_i_id = " + ol_i_id + " AND s_w_id = " + ol_supply_w_id + " FOR UPDATE", e);
                    throw new Exception("NewOrder select transaction error", e);
                }

                ol_dist_info = pickDistInfo(ol_dist_info, d_id);    /* pick correct * s_dist_xx */

                stock[ol_num_seq[ol_number - 1]] = s_quantity;

                if ((i_data.contains("original")) && (s_data.contains("original"))) {
                    bg[ol_num_seq[ol_number - 1]] = "B";

                } else {
                    bg[ol_num_seq[ol_number - 1]] = "G";

                }

                if (s_quantity > ol_quantity) {
                    s_quantity = s_quantity - ol_quantity;
                } else {
                    s_quantity = s_quantity - ol_quantity + 91;
                }

                //Get the prepared statement
                //"UPDATE stock SET s_quantity = ? WHERE s_i_id = ? AND s_w_id = ?"
                try {
                    final PreparedStatement pstmt7 = pStmts.getStatement(7);
                    pstmt7.setInt(1, s_quantity);
                    pstmt7.setInt(2, ol_i_id);
                    pstmt7.setInt(3, ol_supply_w_id);
                    pstmt7.executeUpdate();


                } catch (SQLException e) {
                    logger.error("UPDATE bmsql_stock SET s_quantity = " + s_quantity + " WHERE s_i_id = " + ol_i_id + " AND s_w_id = " + ol_supply_w_id, e);
                    throw new Exception("NewOrder update transaction error", e);
                }

                fields.clear();
                results.clear();
                fields.add("s_quantity");
                fields.add("s_i_id");
                fields.add("s_w_id");
                results.add(Integer.toString(s_quantity));
                results.add(Integer.toString(ol_i_id));
                results.add(Integer.toString(ol_supply_w_id));

                sql = "select * from tpcc.bmsql_stock where s_i_id='" + ol_i_id + "'and s_w_id='" + ol_supply_w_id +"'";
                sqlToFields.put(sql, (ArrayList<String>)fields.clone());
                sqlToResults.put(sql, (ArrayList<String>)results.clone());

                ol_amount = ol_quantity * i_price * (1 + w_tax + d_tax) * (1 - c_discount);
                amt[ol_num_seq[ol_number - 1]] = ol_amount;


                //Get prepared statement
                //"INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"

                try {
                    final PreparedStatement pstmt8 = pStmts.getStatement(8);
                    pstmt8.setInt(1, o_id);
                    pstmt8.setInt(2, d_id);
                    pstmt8.setInt(3, w_id);
                    pstmt8.setInt(4, ol_number);
                    pstmt8.setInt(5, ol_i_id);
                    pstmt8.setInt(6, ol_supply_w_id);
                    pstmt8.setInt(7, ol_quantity);
                    pstmt8.setFloat(8, ol_amount);
                    pstmt8.setString(9, ol_dist_info);
                    pstmt8.executeUpdate();


                } catch (SQLException e) {
                    logger.error("INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info) " +
                            "VALUES (" + o_id + "," + d_id + "," + w_id + "," + ol_number + "," + ol_i_id + "," + ol_supply_w_id + "," + ol_quantity + ","
                            + ol_amount + "," + ol_dist_info + ")", e);
                    throw new Exception("NewOrder insert transaction error", e);
                }
                fields.clear();
                results.clear();
                fields.add("ol_o_id");
                fields.add("ol_d_id");
                fields.add("ol_w_id");
                fields.add("ol_number");
                fields.add("ol_i_id");
                fields.add("ol_supply_w_id");
                fields.add("ol_quantity");
                fields.add("ol_dist_info");
                results.add(Integer.toString(o_id));
                results.add(Integer.toString(d_id));
                results.add(Integer.toString(w_id));
                results.add(Integer.toString(ol_number));
                results.add(Integer.toString(ol_i_id));
                results.add(Integer.toString(ol_supply_w_id));
                results.add(Integer.toString(ol_quantity));
                results.add(ol_dist_info);

                sql = "select * from tpcc.bmsql_order_line where ol_o_id='" + o_id + "'and ol_d_id='" + d_id +"' and ol_w_id='" + w_id + "' and ol_number='" + ol_number + "'";
                sqlToFields.put(sql, (ArrayList<String>)fields.clone());
                sqlToResults.put(sql, (ArrayList<String>)results.clone());

            }
            // Commit.
            pStmts.commit();
            for (Map.Entry<String, ArrayList<String>> entry : sqlToFields.entrySet()) {
                System.out.println(entry.getKey());
                if (checkSparkRead(entry.getKey(), entry.getValue().toArray(new String[0]), sqlToResults.get(entry.getKey()).toArray(new String[0]))) {
                    System.out.println("spark check read right");
                }
            }

            return 1;
        } catch (AbortedTransactionException ate) {
            // Rollback if an aborted transaction, they are intentional in some percentage of cases.
            if (logger.isDebugEnabled()) {
                logger.debug("Caught AbortedTransactionException");
            }
            pStmts.rollback();
            return 1; // this is not an error!
        } catch (Exception e) {
            logger.error("New Order error", e);
            pStmts.rollback();
            return 0;
        }
    }

    private static int doNeword() throws SQLException {
        int c_num = 0;
        int i = 0;

        // warehouse id
        int w_id = 0;
        // district id
        int d_id = 0;
        // customer id
        int c_id = 0;
        // orderline count ?
        int ol_cnt = 0;
        int all_local = 1;
        int notfound = MAXITEMS + 1;
        // flag to determine whether to generate a non-existing item
        int rbk = 0;

        int ret = 0;

        int[] itemid = new int[MAX_NUM_ITEMS];
        int[] supware = new int[MAX_NUM_ITEMS];
        int[] qty = new int[MAX_NUM_ITEMS];

        w_id = Util.randomNumber(1, num_ware);

        if (w_id < 1) {
            throw new IllegalStateException("Invalid warehouse ID " + w_id);
        }


        d_id = Util.randomNumber(1, DIST_PER_WARE);
        c_id = Util.nuRand(1023, 1, CUST_PER_DIST);

        ol_cnt = Util.randomNumber(5, 15);
        rbk = Util.randomNumber(1, 100);

        for (i = 0; i < ol_cnt; i++) {
            itemid[i] = Util.nuRand(8191, 1, MAXITEMS);
            if ((i == ol_cnt - 1) && (rbk == 1)) {
                itemid[i] = notfound;
            }
            supware[i] = w_id;

            qty[i] = Util.randomNumber(1, 10);
        }

        ret = newOrder(w_id, d_id, c_id, ol_cnt, all_local, itemid, supware, qty);


        return ret;
    }

    public static void main(String[] args) throws Exception {
        Class.forName("com.mysql.jdbc.Driver");

        Connection conn = DriverManager.getConnection(URL, NAME, PASSWORD);

        pStmts = new TpccStatements(conn, 1);

        int[] itemid = {1, 2, 3};

        spark = SparkSession
                .builder()
                .master("local")
                .appName("learner read")
                .config(PD_ADDRESSES, "172.16.5.81:13499")
                .config(FLASH_ADDRESSES, "172.16.5.81:8900")
                .config(PARTITION_PER_SPLIT, DEFAULT_PARTITION_PER_SPLIT)
                .config("spark.sql.extensions", "org.apache.spark.sql.CHExtensions")
                .getOrCreate();
        SparkSession.setActiveSession(spark);
        SparkSession.setDefaultSession(spark);
        spark.sparkContext().setLogLevel("WARN");

        doNeword();
    }
}
