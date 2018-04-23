package com.gp.spark.session;

import java.util.*;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.gp.constant.Constants;
import com.gp.dao.*;
import com.gp.domain.SessionAggrStat;
import com.gp.domain.TaskEntity;
import com.gp.factory.DaoFactory;
import com.gp.helper.ConfManager;
import com.gp.spark.data.CreateTestData;
import com.gp.spark.session.domain.SessionDetail;
import com.gp.spark.session.domain.SessionExtractRandom;
import com.gp.spark.session.domain.Top10Entity;
import com.gp.spark.session.utils.SessionAggrStatAccumulator;
import com.gp.spark.session.utils.SortByKeyBySelf;
import com.gp.utils.*;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import scala.Tuple2;
import com.alibaba.fastjson.JSONObject;

/**
 * 用户访问session分析Spark作业
 * <p>
 * 接收用户创建的分析任务，用户可能指定的条件如下：
 * <p>
 * 1、时间范围：起始日期~结束日期
 * 2、性别：男或女
 * 3、年龄范围
 * 4、职业：多选
 * 5、城市：多选
 * 6、搜索词：多个搜索词，只要某个session中的任何一个action搜索过指定的关键词，那么session就符合条件
 * 7、点击品类：多个品类，只要某个session中的任何一个action点击过某个品类，那么session就符合条件
 *
 * @author
 */
public class SessionAnalyze {

    public static void main(String[] args) {
        args = new String[]{"1"};

        // 构建Spark上下文
        SparkConf conf = new SparkConf()
                .setAppName(Constants.SPARK_APP_NAME_SESSION)
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = getSQLContext(sc.sc());

        // 生成模拟测试数据
        mockData(sc, sqlContext);

        // 创建需要使用的DAO组件
        ITaskDAO taskDAO = DaoFactory.getTaskDao();

        // 首先得查询出来指定的任务，并获取任务的查询参数
        long taskid = Long.parseLong(args[0]);

        TaskEntity task = taskDAO.findById(taskid);
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParams());

        // 如果要进行session粒度的数据聚合
        // 首先要从user_visit_action表中，查询出来指定日期范围内的行为数据
        JavaRDD<Row> actionRDD = getActionRDDByDateRange(sqlContext, taskParam);

        JavaPairRDD<String, Row> sessionid2actionRDD = getSessionid2ActionRDD(actionRDD);


        // 到这里为止，获取的数据是<sessionid,(sessionid,searchKeywords,clickCategoryIds,age,professional,city,sex)>  
        JavaPairRDD<String, String> sessionid2AggrInfoRDD =
                aggregateBySession(sqlContext, actionRDD);

        // 重构，同时进行过滤和统计
        Accumulator<String> sessionAggrStatAccumulator = sc.accumulator(
                "", new SessionAggrStatAccumulator());

        JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = filterSessionAndAggrStat(
                sessionid2AggrInfoRDD, taskParam, sessionAggrStatAccumulator);

        System.out.println(filteredSessionid2AggrInfoRDD.count());  //action操作不然为null

        // 计算出各个范围的session占比，并写入MySQL
        calculateAndPersistAggrStat(sessionAggrStatAccumulator.value(),
                task.getTaskId());

        //随机抽取Session并写入mysql
        randomExtractSession(sessionid2AggrInfoRDD, task.getTaskId(), sessionid2actionRDD);

        //获取top并写入mysql
        getTop10Catagory(taskid, filteredSessionid2AggrInfoRDD, sessionid2actionRDD);
        // 关闭Spark上下文
        sc.close();
    }

    /**
     * @param taskid
     * @param filteredSessionid2AggrInfoRDD
     * @param sessionid2actionRDD
     */
    private static void getTop10Catagory(long taskid,
                                         JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD,
                                         JavaPairRDD<String, Row> sessionid2actionRDD) {
        JavaPairRDD<String, Row> joinrdd = filteredSessionid2AggrInfoRDD.join(sessionid2actionRDD).
                mapToPair(new PairFunction<Tuple2<String, Tuple2<String, Row>>, String, Row>() {

                    @Override
                    public Tuple2<String, Row> call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
                        return new Tuple2<String, Row>(tuple._1, tuple._2._2);
                    }
                });

        //获取访问过的品类
        JavaPairRDD<String, String> catardd = joinrdd.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Row>,
                String, String>() {
            @Override
            public Iterable<Tuple2<String, String>> call(Tuple2<String, Row> tuple2) throws Exception {
                Row row = tuple2._2;
                List<Tuple2<String, String>> list = Lists.newArrayList();

                String click = row.getString(6);
                String order = row.getString(8);
                String pay = row.getString(10);
                if (click != null) {
                    list.add(new Tuple2<String, String>(click, click));
                }

                if (order != null) {
                    String[] orders = order.split(",");
                    for (String od : orders) {
                        list.add(new Tuple2<String, String>(od, od));
                    }
                }

                if (pay != null) {
                    String[] pays = pay.split(",");
                    for (String p : pays) {
                        list.add(new Tuple2<String, String>(p, p));
                    }
                }
                return list;
            }
        });
        catardd = catardd.distinct();


        //计算各个类别的各种方式的数量
        JavaPairRDD<String, Long> clickcount = getClickCountRdd(sessionid2actionRDD);
        JavaPairRDD<String, Long> order = getOrderCountRdd(sessionid2actionRDD);
        JavaPairRDD<String, Long> pary = getPayCountRdd(sessionid2actionRDD);


        //第三步：join各品类与它的点击、下单和支付的次数
        JavaPairRDD<String, String> joinCatagoryandData = joinCatagoryandData(catardd, clickcount, order, pary);

        //将数据映射成<CategorySortKey,info>格式的RDD，然后进行二次排序（降序）

        JavaPairRDD<SortByKeyBySelf, String> sortByKeyBySelfStringJavaPairRDD =
                joinCatagoryandData.mapToPair(new PairFunction<Tuple2<String, String>, SortByKeyBySelf, String>() {
                    @Override
                    public Tuple2<SortByKeyBySelf, String> call(Tuple2<String, String> tuple2)
                            throws Exception {

                        String info = tuple2._2;
                        long clickCount = Long.valueOf(StringUtils.getFieldFromConcatString(info,
                                "\\|", Constants.FIELD_CLICK_COUNT));
                        long orderCount = Long.valueOf(StringUtils.getFieldFromConcatString(info,
                                "\\|", Constants.FIELD_ORDER_COUNT));
                        long payCount = Long.valueOf(StringUtils.getFieldFromConcatString(info,
                                "\\|", Constants.FIELD_PAY_COUNT));

                        SortByKeyBySelf sortByKeyBySelf = new SortByKeyBySelf();
                        sortByKeyBySelf.setClickCount(clickCount);
                        sortByKeyBySelf.setOrderCount(orderCount);
                        sortByKeyBySelf.setPayCount(payCount);
                        return new Tuple2<SortByKeyBySelf, String>(sortByKeyBySelf, info);
                    }
                });

        JavaPairRDD<SortByKeyBySelf, String> sortbykeyrdd = sortByKeyBySelfStringJavaPairRDD.sortByKey(false);


        List<Tuple2<SortByKeyBySelf, String>> list = sortbykeyrdd.take(10);

        for (Tuple2<SortByKeyBySelf, String> tuple2 : list) {
            String countInfo = tuple2._2;
            //System.out.println(countInfo);
            long categoryid = Long.valueOf(StringUtils.getFieldFromConcatString(
                    countInfo, "\\|", Constants.FIELD_CATEGORY_ID));
            long clickCount = Long.valueOf(StringUtils.getFieldFromConcatString(
                    countInfo, "\\|", Constants.FIELD_CLICK_COUNT));
            long orderCount = Long.valueOf(StringUtils.getFieldFromConcatString(
                    countInfo, "\\|", Constants.FIELD_ORDER_COUNT));
            long payCount = Long.valueOf(StringUtils.getFieldFromConcatString(
                    countInfo, "\\|", Constants.FIELD_PAY_COUNT));


            Top10Entity entity = new Top10Entity();
            entity.setTaskid(taskid);
            entity.setClickCount(clickCount);
            entity.setCategoryid(categoryid);
            entity.setOrderCount(orderCount);
            entity.setPayCount(payCount);

            ITop10DAO itop10 = DaoFactory.getItop10();
            itop10.insert(entity);
        }

    }


    //###################
    private static JavaPairRDD<String, Long> getClickCountRdd(JavaPairRDD<String, Row>
                                                                      sessionid2actionRDD) {
        JavaPairRDD<String, Row> filter = sessionid2actionRDD.filter(new Function<Tuple2<String, Row>,
                Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Row> v1) throws Exception {
                String clickid = v1._2.getString(6);
                return clickid != null ? true : false;
            }
        });
        JavaPairRDD<String, Long> pairRDD = filter.mapToPair(new PairFunction<Tuple2<String, Row>,
                String, Long>() {
            @Override
            public Tuple2<String, Long> call(Tuple2<String, Row> tuple2) throws Exception {
                Row row = tuple2._2;
                String clickid = row.getString(6);
                return new Tuple2<String, Long>(clickid, 1l);
            }
        });

        JavaPairRDD<String, Long> reduceByKeyrdd = pairRDD.reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long v1, Long v2) throws Exception {
                return v1 + v2;
            }
        });
        return reduceByKeyrdd;
    }

    private static JavaPairRDD<String, Long> getOrderCountRdd(JavaPairRDD<String, Row>
                                                                      sessionid2actionRDD) {
        JavaPairRDD<String, Row> filterRdd = sessionid2actionRDD.filter(new Function<Tuple2<String, Row>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Row> v1) throws Exception {
                return (v1._2.getString(8)) != null;
            }
        });
        JavaPairRDD<String, Long> flatMapToPair = filterRdd.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Row>,
                String, Long>() {
            @Override
            public Iterable<Tuple2<String, Long>> call(Tuple2<String, Row> tuple2) throws Exception {
                Row row = tuple2._2;

                List<Tuple2<String, Long>> list = Lists.newArrayList();
                String[] orderStrs = row.getString(8).split(",");
                for (String od : orderStrs) {
                    list.add(new Tuple2<String, Long>(od, 1l));
                }

                return list;
            }
        });

        JavaPairRDD<String, Long> reduceByKey = flatMapToPair.reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long v1, Long v2) throws Exception {
                return v1 + v2;
            }
        });

        return reduceByKey;
    }

    private static JavaPairRDD<String, Long> getPayCountRdd(JavaPairRDD<String, Row>
                                                                    sessionid2actionRDD) {
        JavaPairRDD<String, Row> filter = sessionid2actionRDD.filter(new Function<Tuple2<String, Row>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Row> v1) throws Exception {
                return v1._2.getString(10) != null;
            }
        });

        JavaPairRDD<String, Long> flatMapToPair = filter.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Row>, String, Long>() {
            @Override
            public Iterable<Tuple2<String, Long>> call(Tuple2<String, Row> tuple2) throws Exception {
                Row row = tuple2._2;
                String[] strs = row.getString(10).split(",");
                List<Tuple2<String, Long>> list = Lists.newArrayList();
                for (String s : strs) {
                    list.add(new Tuple2<String, Long>(s, 1l));
                }
                return list;
            }
        });

        //flatMapToPair = flatMapToPair.distinct();

        JavaPairRDD<String, Long> reduceByKey = flatMapToPair.reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long v1, Long v2) throws Exception {
                return v1 + v2;
            }
        });
        return reduceByKey;
    }


    private static JavaPairRDD<String, String> joinCatagoryandData(JavaPairRDD<String, String> catardd,
                                                                   JavaPairRDD<String, Long> clickcount,
                                                                   JavaPairRDD<String, Long> order,
                                                                   JavaPairRDD<String, Long> pary) {

        JavaPairRDD<String, Tuple2<String, Optional<Long>>> leftOuterJoin = catardd.leftOuterJoin(clickcount);
        //System.out.println(leftOuterJoin.count());

        JavaPairRDD<String, String> temprdd = leftOuterJoin.mapToPair(new PairFunction<Tuple2<String,
                Tuple2<String, Optional<Long>>>, String,
                String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<String, Tuple2<String, Optional<Long>>> tuple2)
                    throws Exception {
                String catagoryid = tuple2._1;
                Optional<Long> optional = tuple2._2._2;
                Long clickCount = 0l;
                if (optional.isPresent()) {
                    clickCount = optional.get();
                }

                String value = Constants.FIELD_CATEGORY_ID + "=" + catagoryid + "|" +
                        Constants.FIELD_CLICK_COUNT + "=" + clickCount;


                return new Tuple2<String, String>(catagoryid, value);
            }
        });

        temprdd = temprdd.leftOuterJoin(order).mapToPair(new PairFunction<Tuple2<String, Tuple2<String, Optional<Long>>>,
                String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<String, Tuple2<String, Optional<Long>>> tuple) throws Exception {

                String categoryid = tuple._1;
                String value = tuple._2._1;

                Optional<Long> optional = tuple._2._2;
                long orderCount = 0L;

                if (optional.isPresent()) {
                    orderCount = optional.get();
                }

                value = value + "|" + Constants.FIELD_ORDER_COUNT + "=" + orderCount;

                return new Tuple2<String, String>(categoryid, value);
            }
        });

        temprdd = temprdd.leftOuterJoin(pary).mapToPair(new PairFunction<Tuple2<String, Tuple2<String, Optional<Long>>>,
                String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<String, Tuple2<String, Optional<Long>>> tuple)
                    throws Exception {
                String categoryid = tuple._1;
                String value = tuple._2._1;

                Optional<Long> optional = tuple._2._2;
                long payCount = 0L;

                if (optional.isPresent()) {
                    payCount = optional.get();
                }

                value = value + "|" + Constants.FIELD_PAY_COUNT + "=" + payCount;

                return new Tuple2<String, String>(categoryid, value);
            }
        });

        return temprdd;
    }
//####################

    /**
     * @param actionRDD 用户行为数据
     * @return
     */
    private static JavaPairRDD<String, Row> getSessionid2ActionRDD(JavaRDD<Row> actionRDD) {
        return actionRDD.mapToPair(new PairFunction<Row, String, Row>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {
                return new Tuple2<String, Row>(row.getString(2), row);
            }

        });
    }

    /**
     * 随机抽取Session
     *
     * @param sessionid2AggrInfoRDD
     */
    private static void randomExtractSession(JavaPairRDD<String, String> sessionid2AggrInfoRDD,
                                             final long taskid,
                                             JavaPairRDD<String, Row> sessionid2actionRDD) {

        JavaPairRDD<String, String> mapToPair = sessionid2AggrInfoRDD.mapToPair(new PairFunction<Tuple2<String,
                String>, String, String>() {

            @Override
            public Tuple2<String, String> call(Tuple2<String, String> tuple2) throws Exception {
                String aggFullInfo = tuple2._2;
                String start_time = StringUtils.getFieldFromConcatString(aggFullInfo,
                        "\\|", Constants.FIELD_START_TIME);
                String hourkey = DateUtils.getDateHour(start_time);
                return new Tuple2<String, String>(hourkey, aggFullInfo);
            }
        });

        //得到每小时的Session数量
        Map<String, Object> countByhour = mapToPair.countByKey();

        //测试
       /* for (Map.Entry<String, Object> entry : countByhour.entrySet()) {
            System.out.println("key=" + entry.getKey() + ",value=" + entry.getValue());
        }*/

        //// 将<yyyy-MM-dd_HH,count>格式的map，转换成<yyyy-MM-dd,<HH,count>>的格式

        Map<String, Map<String, Long>> dateMap = new HashMap<String, Map<String, Long>>();
        for (Map.Entry<String, Object> entry : countByhour.entrySet()) {
            String date = entry.getKey().split("_")[0];
            String hour = entry.getKey().split("_")[1];
            Long count = Long.valueOf(String.valueOf(entry.getValue()));

            Map<String, Long> hourMap = dateMap.get(date);  //只统计当天的
            if (hourMap == null) {
                hourMap = new HashMap<String, Long>();
                dateMap.put(date, hourMap);
            }
            hourMap.put(hour, count);
        }

        //测试
        /*for (Map.Entry<String, Map<String, Object>> entry : dateMap.entrySet()) {
            Map<String, Object> value = entry.getValue();
            for (Map.Entry<String, Object> val : value.entrySet()) {
                System.out.println("key1=" + entry.getKey() + ",key2=" + val.getKey()
                        + ",value=" + val.getValue());
            }
        }*/


        final Map<String, Map<String, List<Integer>>> dataExtractMap = new HashMap<String,
                Map<String, List<Integer>>>();
        //总共要抽取100个session ，进行每天的平均；
        int sessionPerDate = 100 / dateMap.size();
        Random random = new Random();

        // <date,<hour,(3,5,20,102)>>
        for (Map.Entry<String, Map<String, Long>> entry : dateMap.entrySet()) {
            String date = entry.getKey();
            Map<String, Long> valueMap = entry.getValue();

            //统计总数
            long sessionCount = 0l;
            for (long hourCount : valueMap.values()) {
                sessionCount += hourCount;
            }

            Map<String, List<Integer>> hourExtracMap = dataExtractMap.get(date);
            if (hourExtracMap == null) {
                hourExtracMap = new HashMap<String, List<Integer>>();
                dataExtractMap.put(date, hourExtracMap);
            }


            for (Map.Entry<String, Long> en : valueMap.entrySet()) {
                String hour = en.getKey();
                long hourcount = en.getValue();

                // 就可以计算出，当前小时需要抽取的session数量
                int hourExtractNumber = (int) (((double) hourcount / (double) sessionCount)
                        * sessionPerDate);
                if (hourExtractNumber > hourcount) {
                    hourExtractNumber = (int) hourcount;
                }

                List<Integer> al = hourExtracMap.get(hour);
                if (al == null) {
                    al = new ArrayList<Integer>();
                    hourExtracMap.put(hour, al);
                }

                //生成随机索引
                for (int i = 0; i < hourExtractNumber; i++) {
                    int extracIndex = random.nextInt((int) hourcount);
                    if (al.contains(extracIndex)) {
                        extracIndex = random.nextInt((int) hourcount);
                    }
                    al.add(extracIndex);
                }
            }
        }

        //测试
     /*for (Map.Entry<String, Map<String, List<Integer>>> entry : dataExtractMap.entrySet()) {
            Map<String, List<Integer>> value = entry.getValue();
            for (Map.Entry<String, List<Integer>> val : value.entrySet()) {

                System.out.println("key1=" + entry.getKey() + ",key2=" + val.getKey()
                        + ",value=" + val.getValue());
            }
        }
       */

        JavaPairRDD<String, Iterable<String>> session2rdd = mapToPair.groupByKey();

        JavaPairRDD<String, String> extractSessionidsRDD = session2rdd.flatMapToPair(new PairFlatMapFunction<Tuple2<String,
                Iterable<String>>, String, String>() {

            @Override
            public Iterable<Tuple2<String, String>> call(Tuple2<String, Iterable<String>> tuple2) throws Exception {


                List<Tuple2<String, String>> extractSessionids =
                        new ArrayList<Tuple2<String, String>>();

                String dateHour = tuple2._1;
                String date = dateHour.split("_")[0];
                String hour = dateHour.split("_")[1];

                Iterator<String> iterator = tuple2._2.iterator();

                IRandExtactSessionDao randExtractSession = DaoFactory.getRandExtractSession();
                //索引
                List<Integer> listIndex = dataExtractMap.get(date).get(hour);

                int index = 0;
                while (iterator.hasNext()) {
                    String agginfo = iterator.next();
                    if (listIndex.contains(index)) {
                        String session_id = StringUtils.getFieldFromConcatString(agginfo,
                                "\\|", Constants.FIELD_SESSION_ID);
                        String startTime = StringUtils.getFieldFromConcatString(agginfo,
                                "\\|", Constants.FIELD_START_TIME);
                        String catagroy_id = StringUtils.getFieldFromConcatString(agginfo,
                                "\\|", Constants.FIELD_CATEGORY_ID);
                        String search_keywords = StringUtils.getFieldFromConcatString(agginfo,
                                "\\|", Constants.FIELD_SEARCH_KEYWORDS);

                        SessionExtractRandom sessionExtractRandom = new SessionExtractRandom();
                        sessionExtractRandom.setCatagroy_id(catagroy_id);
                        sessionExtractRandom.setSearch_keywords(search_keywords);
                        sessionExtractRandom.setStart_time(startTime);
                        sessionExtractRandom.setSession_id(session_id);
                        sessionExtractRandom.setTask_id(taskid);

                        randExtractSession.insert(sessionExtractRandom);

                        extractSessionids.add(new Tuple2<String, String>(session_id, session_id));
                    }
                    index++;
                }
                return extractSessionids;
            }
        });

        //System.out.println(extractSessionidsRDD.count());

        JavaPairRDD<String, Tuple2<String, Row>> extractSessionDetailRDD =
                extractSessionidsRDD.join(sessionid2actionRDD);

        extractSessionDetailRDD.foreach(new VoidFunction<Tuple2<String, Tuple2<String, Row>>>() {

            private static final long serialVersionUID = 1L;

            @Override
            public void call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
                Row row = tuple._2._2;

                SessionDetail sessionDetail = new SessionDetail();
                sessionDetail.setTask_id(taskid);
                sessionDetail.setUser_id(row.getLong(1));
                sessionDetail.setSession_id(row.getString(2));
                sessionDetail.setPage_id(row.getLong(3));
                sessionDetail.setAction_time(row.getString(4));
                sessionDetail.setSearch_keyword(row.getString(5));
                sessionDetail.setClick_category_id(row.getString(6));
                sessionDetail.setClick_product_id(row.getString(7));
                sessionDetail.setOrder_category_ids(row.getString(8));
                sessionDetail.setOrder_product_ids(row.getString(9));
                sessionDetail.setPay_category_ids(row.getString(10));
                sessionDetail.setPay_product_ids(row.getString(11));

                ISessionDetailDAO sessionDetailDAO = DaoFactory.getSessionDetail();
                sessionDetailDAO.insert(sessionDetail);
            }
        });
    }

    /**
     * 获取SQLContext
     * 如果是在本地测试环境的话，那么就生成SQLContext对象
     * 如果是在生产环境运行的话，那么就生成HiveContext对象
     *
     * @param sc SparkContext
     * @return SQLContext
     */
    private static SQLContext getSQLContext(SparkContext sc) {
        boolean local = ConfManager.getBoolean(Constants.SPARK_LOCAL);
        if (local) {
            return new SQLContext(sc);
        } else {
            return new HiveContext(sc);
        }
    }

    /**
     * 生成模拟数据（只有本地模式，才会去生成模拟数据）
     *
     * @param sc
     * @param sqlContext
     */
    private static void mockData(JavaSparkContext sc, SQLContext sqlContext) {
        boolean local = ConfManager.getBoolean(Constants.SPARK_LOCAL);
        if (local) {
            CreateTestData.mock(sc, sqlContext);
        }
    }

    /**
     * 获取指定日期范围内的用户访问行为数据
     *
     * @param sqlContext SQLContext
     * @param taskParam  任务参数
     * @return 行为数据RDD
     */
    private static JavaRDD<Row> getActionRDDByDateRange(
            SQLContext sqlContext, JSONObject taskParam) {
        System.out.println(taskParam);
        String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);

        String sql =
                "select * "
                        + "from user_visit_action "
                        + "where date>='" + startDate + "' "
                        + "and date<='" + endDate + "'";

        DataFrame actionDF = sqlContext.sql(sql);

        return actionDF.javaRDD();
    }

    /**
     * 对行为数据按session粒度进行聚合
     *
     * @param actionRDD 行为数据RDD
     * @return session粒度聚合数据
     */
    private static JavaPairRDD<String, String> aggregateBySession(
            SQLContext sqlContext, JavaRDD<Row> actionRDD) {
        // 我们现在需要将这个Row映射成<sessionid,Row>的格式
        JavaPairRDD<String, Row> sessionid2ActionRDD = actionRDD.mapToPair(


                new PairFunction<Row, String, Row>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<String, Row> call(Row row) throws Exception {
                        return new Tuple2<String, Row>(row.getString(2), row);
                    }

                });

        // 对行为数据按session粒度进行分组
        JavaPairRDD<String, Iterable<Row>> sessionid2ActionsRDD =
                sessionid2ActionRDD.groupByKey();

        // 对每一个session分组进行聚合，将session中所有的搜索词和点击品类都聚合起来
        // 如下：<userid,partAggrInfo(sessionid,searchKeywords,clickCategoryIds)>
        JavaPairRDD<Long, String> userid2PartAggrInfoRDD = sessionid2ActionsRDD.mapToPair(

                new PairFunction<Tuple2<String, Iterable<Row>>, Long, String>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<Long, String> call(Tuple2<String, Iterable<Row>> tuple)
                            throws Exception {
                        String sessionid = tuple._1;
                        Iterator<Row> iterator = tuple._2.iterator();

                        StringBuffer searchKeywordsBuffer = new StringBuffer("");
                        StringBuffer clickCategoryIdsBuffer = new StringBuffer("");

                        Long userid = null;

                        // session的起始和结束时间
                        Date startTime = null;
                        Date endTime = null;
                        // session的访问步长
                        int stepLength = 0;

                        // 遍历session所有的访问行为
                        while (iterator.hasNext()) {
                            // 提取每个访问行为的搜索词字段和点击品类字段
                            Row row = iterator.next();
                            if (userid == null) {
                                userid = row.getLong(1);
                            }
                            String searchKeyword = row.getString(5);
                            String clickCategoryId = row.getString(6);  //这里如果使用Long，可能会导致 空指针异常

                            if (StringUtils.isNotEmpty(searchKeyword)) {
                                if (!searchKeywordsBuffer.toString().contains(searchKeyword)) {
                                    searchKeywordsBuffer.append(searchKeyword + ",");
                                }
                            }
                            if (StringUtils.isNotEmpty(clickCategoryId)) {
                                if (!clickCategoryIdsBuffer.toString().contains(
                                        clickCategoryId)) {
                                    clickCategoryIdsBuffer.append(clickCategoryId + ",");
                                }
                            }

                            // 计算session开始和结束时间
                            Date actionTime = DateUtils.parseTime(row.getString(4));

                            if (startTime == null) {
                                startTime = actionTime;
                            }
                            if (endTime == null) {
                                endTime = actionTime;
                            }

                            if (actionTime.before(startTime)) {
                                startTime = actionTime;
                            }
                            if (actionTime.after(endTime)) {
                                endTime = actionTime;
                            }

                            // 计算session访问步长
                            stepLength++;
                        }

                        String searchKeywords = StringUtils.trimComma(searchKeywordsBuffer.toString());
                        String clickCategoryIds = StringUtils.trimComma(clickCategoryIdsBuffer.toString());

                        // 计算session访问时长（秒）
                        long visitLength = (endTime.getTime() - startTime.getTime()) / 1000;

                        // 我们这里统一定义，使用key=value|key=value
                        String partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionid + "|"
                                + Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeywords + "|"
                                + Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds + "|"
                                + Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|"
                                + Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|"
                                + Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime);

                        return new Tuple2<Long, String>(userid, partAggrInfo);
                    }

                });

        // 查询所有用户数据，并映射成<userid,Row>的格式
        String sql = "select * from user_info";
        JavaRDD<Row> userInfoRDD = sqlContext.sql(sql).javaRDD();

        JavaPairRDD<Long, Row> userid2InfoRDD = userInfoRDD.mapToPair(

                new PairFunction<Row, Long, Row>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<Long, Row> call(Row row) throws Exception {
                        return new Tuple2<Long, Row>(row.getLong(0), row);
                    }

                });

        //将session粒度聚合数据，与用户信息进行join
        JavaPairRDD<Long, Tuple2<String, Row>> userid2FullInfoRDD =
                userid2PartAggrInfoRDD.join(userid2InfoRDD);

        // 对join起来的数据进行拼接，并且返回<sessionid,fullAggrInfo>格式的数据
        JavaPairRDD<String, String> sessionid2FullAggrInfoRDD = userid2FullInfoRDD.mapToPair(

                new PairFunction<Tuple2<Long, Tuple2<String, Row>>, String, String>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<String, String> call(
                            Tuple2<Long, Tuple2<String, Row>> tuple)
                            throws Exception {
                        String partAggrInfo = tuple._2._1;
                        Row userInfoRow = tuple._2._2;

                        String sessionid = StringUtils.getFieldFromConcatString(
                                partAggrInfo, "\\|", Constants.FIELD_SESSION_ID);

                        int age = userInfoRow.getInt(3);
                        String professional = userInfoRow.getString(4);
                        String city = userInfoRow.getString(5);
                        String sex = userInfoRow.getString(6);

                        String fullAggrInfo = partAggrInfo + "|"
                                + Constants.FIELD_AGE + "=" + age + "|"
                                + Constants.FIELD_PROFESSIONAL + "=" + professional + "|"
                                + Constants.FIELD_CITY + "=" + city + "|"
                                + Constants.FIELD_SEX + "=" + sex;

                        return new Tuple2<String, String>(sessionid, fullAggrInfo);
                    }

                });

        return sessionid2FullAggrInfoRDD;
    }

    /**
     * 过滤session数据，并进行聚合统计
     *
     * @param sessionid2AggrInfoRDD
     * @return
     */
    private static JavaPairRDD<String, String> filterSessionAndAggrStat(
            JavaPairRDD<String, String> sessionid2AggrInfoRDD,
            final JSONObject taskParam,
            final Accumulator<String> sessionAggrStatAccumulator) {


        String startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE);
        String endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE);
        String professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS);
        String cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES);
        String sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX);
        String keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS);
        String categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS);

        String _parameter = (startAge != null ? Constants.PARAM_START_AGE + "=" + startAge + "|" : "")
                + (endAge != null ? Constants.PARAM_END_AGE + "=" + endAge + "|" : "")
                + (professionals != null ? Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" : "")
                + (cities != null ? Constants.PARAM_CITIES + "=" + cities + "|" : "")
                + (sex != null ? Constants.PARAM_SEX + "=" + sex + "|" : "")
                + (keywords != null ? Constants.PARAM_KEYWORDS + "=" + keywords + "|" : "")
                + (categoryIds != null ? Constants.PARAM_CATEGORY_IDS + "=" + categoryIds : "");

        if (_parameter.endsWith("\\|")) {
            _parameter = _parameter.substring(0, _parameter.length() - 1);
        }

        final String parameter = _parameter;

        // 根据筛选参数进行过滤
        JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = sessionid2AggrInfoRDD.filter(

                new Function<Tuple2<String, String>, Boolean>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Boolean call(Tuple2<String, String> tuple) throws Exception {
                        // 首先，从tuple中，获取聚合数据
                        String aggrInfo = tuple._2;

                        // 接着，依次按照筛选条件进行过滤
                        // 按照年龄范围进行过滤（startAge、endAge）
                        if (!ValidUtils.between(aggrInfo, Constants.FIELD_AGE,
                                parameter, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) {
                            return false;
                        }

                        // 按照职业范围进行过滤（professionals）
                        // 互联网,IT,软件
                        // 互联网
                        if (!ValidUtils.in(aggrInfo, Constants.FIELD_PROFESSIONAL,
                                parameter, Constants.PARAM_PROFESSIONALS)) {
                            return false;
                        }

                        // 按照城市范围进行过滤（cities）
                        // 北京,上海,广州,深圳
                        // 成都
                        if (!ValidUtils.in(aggrInfo, Constants.FIELD_CITY,
                                parameter, Constants.PARAM_CITIES)) {
                            return false;
                        }

                        // 按照性别进行过滤
                        // 男/女
                        // 男，女
                        if (!ValidUtils.equal(aggrInfo, Constants.FIELD_SEX,
                                parameter, Constants.PARAM_SEX)) {
                            return false;
                        }

                        // 按照搜索词进行过滤
                        if (!ValidUtils.in(aggrInfo, Constants.FIELD_SEARCH_KEYWORDS,
                                parameter, Constants.PARAM_KEYWORDS)) {
                            return false;
                        }

                        // 按照点击品类id进行过滤
                        if (!ValidUtils.in(aggrInfo, Constants.FIELD_CLICK_CATEGORY_IDS,
                                parameter, Constants.PARAM_CATEGORY_IDS)) {
                            return false;
                        }

                        sessionAggrStatAccumulator.add(Constants.SESSION_COUNT);

                        // 计算出session的访问时长和访问步长的范围，并进行相应的累加
                        long visitLength = Long.valueOf(StringUtils.getFieldFromConcatString(
                                aggrInfo, "\\|", Constants.FIELD_VISIT_LENGTH));
                        long stepLength = Long.valueOf(StringUtils.getFieldFromConcatString(
                                aggrInfo, "\\|", Constants.FIELD_STEP_LENGTH));
                        calculateVisitLength(visitLength);
                        calculateStepLength(stepLength);

                        return true;
                    }

                    /**
                     * 计算访问时长范围
                     * @param visitLength
                     */
                    private void calculateVisitLength(long visitLength) {
                        if (visitLength >= 1 && visitLength <= 3) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1s_3s);
                        } else if (visitLength >= 4 && visitLength <= 6) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_4s_6s);
                        } else if (visitLength >= 7 && visitLength <= 9) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_7s_9s);
                        } else if (visitLength >= 10 && visitLength <= 30) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10s_30s);
                        } else if (visitLength > 30 && visitLength <= 60) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30s_60s);
                        } else if (visitLength > 60 && visitLength <= 180) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1m_3m);
                        } else if (visitLength > 600 && visitLength <= 1800) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10m_30m);
                        } else if (visitLength > 1800) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30m);
                        }
                    }

                    /**
                     * 计算访问步长范围
                     * @param stepLength
                     */
                    private void calculateStepLength(long stepLength) {
                        if (stepLength >= 1 && stepLength <= 3) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_1_3);
                        } else if (stepLength >= 4 && stepLength <= 6) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_4_6);
                        } else if (stepLength >= 7 && stepLength <= 9) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_7_9);
                        } else if (stepLength >= 10 && stepLength <= 30) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_10_30);
                        } else if (stepLength > 30 && stepLength <= 60) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_30_60);
                        } else if (stepLength > 60) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_60);
                        }
                    }

                });

        return filteredSessionid2AggrInfoRDD;
    }

    /**
     * 计算各session范围占比，并写入MySQL
     *
     * @param value
     */
    private static void calculateAndPersistAggrStat(String value, long taskid) {
        // 从Accumulator统计串中获取值
        long session_count = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.SESSION_COUNT));

        long visit_length_1s_3s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_1s_3s));
        long visit_length_4s_6s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_4s_6s));
        long visit_length_7s_9s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_7s_9s));
        long visit_length_10s_30s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_10s_30s));
        long visit_length_30s_60s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_30s_60s));
        long visit_length_1m_3m = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_1m_3m));
        long visit_length_10m_30m = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_10m_30m));
        long visit_length_30m = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_30m));
        long step_length_1_3 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_1_3));
        long step_length_4_6 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_4_6));
        long step_length_7_9 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_7_9));
        long step_length_10_30 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_10_30));
        long step_length_30_60 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_30_60));
        long step_length_60 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_60));

        // 计算各个访问时长和访问步长的范围
        double visit_length_1s_3s_ratio = NumberUtils.formatDouble(
                (double) visit_length_1s_3s / (double) session_count, 2);
        double visit_length_4s_6s_ratio = NumberUtils.formatDouble(
                (double) visit_length_4s_6s / (double) session_count, 2);
        double visit_length_7s_9s_ratio = NumberUtils.formatDouble(
                (double) visit_length_7s_9s / (double) session_count, 2);
        double visit_length_10s_30s_ratio = NumberUtils.formatDouble(
                (double) visit_length_10s_30s / (double) session_count, 2);
        double visit_length_30s_60s_ratio = NumberUtils.formatDouble(
                (double) visit_length_30s_60s / (double) session_count, 2);
        double visit_length_1m_3m_ratio = NumberUtils.formatDouble(
                (double) visit_length_1m_3m / (double) session_count, 2);
        double visit_length_10m_30m_ratio = NumberUtils.formatDouble(
                (double) visit_length_10m_30m / (double) session_count, 2);
        double visit_length_30m_ratio = NumberUtils.formatDouble(
                (double) visit_length_30m / (double) session_count, 2);

        double step_length_1_3_ratio = NumberUtils.formatDouble(
                (double) step_length_1_3 / (double) session_count, 2);
        double step_length_4_6_ratio = NumberUtils.formatDouble(
                (double) step_length_4_6 / (double) session_count, 2);
        double step_length_7_9_ratio = NumberUtils.formatDouble(
                (double) step_length_7_9 / (double) session_count, 2);
        double step_length_10_30_ratio = NumberUtils.formatDouble(
                (double) step_length_10_30 / (double) session_count, 2);
        double step_length_30_60_ratio = NumberUtils.formatDouble(
                (double) step_length_30_60 / (double) session_count, 2);
        double step_length_60_ratio = NumberUtils.formatDouble(
                (double) step_length_60 / (double) session_count, 2);

        // 将统计结果封装为Domain对象
        SessionAggrStat sessionAggrStat = new SessionAggrStat();
        sessionAggrStat.setTaskid(taskid);
        sessionAggrStat.setSession_count(session_count);
        sessionAggrStat.setVisit_length_1s_3s_ratio(visit_length_1s_3s_ratio);
        sessionAggrStat.setVisit_length_4s_6s_ratio(visit_length_4s_6s_ratio);
        sessionAggrStat.setVisit_length_7s_9s_ratio(visit_length_7s_9s_ratio);
        sessionAggrStat.setVisit_length_10s_30s_ratio(visit_length_10s_30s_ratio);
        sessionAggrStat.setVisit_length_30s_60s_ratio(visit_length_30s_60s_ratio);
        sessionAggrStat.setVisit_length_1m_3m_ratio(visit_length_1m_3m_ratio);
        sessionAggrStat.setVisit_length_10m_30m_ratio(visit_length_10m_30m_ratio);
        sessionAggrStat.setVisit_length_30m_ratio(visit_length_30m_ratio);
        sessionAggrStat.setStep_length_1_3_ratio(step_length_1_3_ratio);
        sessionAggrStat.setStep_length_4_6_ratio(step_length_4_6_ratio);
        sessionAggrStat.setStep_length_7_9_ratio(step_length_7_9_ratio);
        sessionAggrStat.setStep_length_10_30_ratio(step_length_10_30_ratio);
        sessionAggrStat.setStep_length_30_60_ratio(step_length_30_60_ratio);
        sessionAggrStat.setStep_length_60_ratio(step_length_60_ratio);

        // 调用对应的DAO插入统计结果
        ISessionDAO sessionAggrStatDAO = DaoFactory.getSessionAggrStatDAO();
        sessionAggrStatDAO.insert(sessionAggrStat);
    }

}
