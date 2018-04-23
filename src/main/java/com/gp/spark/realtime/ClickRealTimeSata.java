package com.gp.spark.realtime;

import com.google.common.collect.Lists;
import com.gp.constant.Constants;
import com.gp.dao.ClickCountDao;
import com.gp.dao.IAdBlacklistDAO;
import com.gp.dao.IAdClickTrendDAO;
import com.gp.spark.realtime.domain.AdBlacklist;
import com.gp.factory.DaoFactory;
import com.gp.helper.ConfManager;
import com.gp.spark.realtime.domain.AdClickTrend;
import com.gp.spark.realtime.domain.ClickCount;
import com.gp.utils.DateUtils;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import com.google.common.base.Optional;
import scala.Tuple2;

import java.util.*;

/**
 * describe:实时统计模块
 *
 * @author huangjia
 * @date 2018/3/21
 */
public class ClickRealTimeSata {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("realTime");
        //每个实时计算的三个步骤：启动初始化，等待，结束关闭；
        JavaStreamingContext jsc =
                new JavaStreamingContext(conf, Durations.seconds(5));

        //zk地址
        Map<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put(Constants.KAFKA_METADATA_BROKER_LIST,
                ConfManager.getPropertis(Constants.KAFKA_METADATA_BROKER_LIST));

        //topic
        Set<String> topicsSet = new HashSet<String>();
        String[] topics = ConfManager.getPropertis(Constants.KAFKA_TOPICS).split(",");
        for (String topic : topics) {
            topicsSet.add(topic);
        }

        //基于kafka的数据源DStream
        JavaPairInputDStream<String, String> directStream = KafkaUtils.createDirectStream(jsc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams, topicsSet);

        // 根据动态黑名单进行数据过滤
        JavaPairDStream<String, String> filteredAdRealTimeLogDStream =
                filterByBlacklist(directStream);

        // 生成动态黑名单
        generateDynamicBlacklist(filteredAdRealTimeLogDStream);


        // 业务功能三：实时统计每天每个广告在最近1小时的滑动窗口内的点击趋势（每分钟的点击量）
        // 统计的非常细了
        // 我们每次都可以看到每个广告，最近一小时内，每分钟的点击量
        // 每支广告的点击趋势
        calculateAdClickCountByWindow(directStream);

        jsc.start();
        jsc.awaitTermination();
    }

    private static void calculateAdClickCountByWindow(JavaPairDStream<String, String>
                                                              Dstream) {

        JavaPairDStream<String, Long> pairDStream = Dstream.mapToPair(new PairFunction<Tuple2<String, String>,
                String, Long>() {

            @Override
            public Tuple2<String, Long> call(Tuple2<String, String> tuple2) throws Exception {
                String[] strings = tuple2._2.split(" ");
                String timeStemp = DateUtils.formatTimeMinute(new Date(Long.valueOf(strings[0])));
                long add_id = Long.valueOf(strings[4]);

                return new Tuple2<String, Long>(timeStemp + "_" + add_id, 1L);
            }
        });

        JavaPairDStream<String, Long> reduceByKeyAndWindow = pairDStream.reduceByKeyAndWindow(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long v1, Long v2) throws Exception {
                return v1 + v2;
            }
        }, Durations.minutes(60), Durations.seconds(10)); //窗口大小是60分钟。每10,秒计算一下窗口长度数据


        reduceByKeyAndWindow.foreachRDD(new Function<JavaPairRDD<String, Long>, Void>() {
            @Override
            public Void call(JavaPairRDD<String, Long> rdd) throws Exception {
                rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Long>>>() {
                    @Override
                    public void call(Iterator<Tuple2<String, Long>> iterator) throws Exception {
                        List<AdClickTrend> al = Lists.newArrayList();
                        while (iterator.hasNext()) {
                            Tuple2<String, Long> tuple = iterator.next();
                            String[] keySplited = tuple._1.split("_");
                            // yyyyMMddHHmm
                            String dateMinute = keySplited[0];
                            long adid = Long.valueOf(keySplited[1]);
                            long clickCount = tuple._2;

                            String date = DateUtils.formatDate(
                                    DateUtils.parseDateKey(dateMinute.substring(0, 8)));
                            String hour = dateMinute.substring(8, 10);
                            String minute = dateMinute.substring(10);

                            AdClickTrend adClickTrend = new AdClickTrend();
                            adClickTrend.setDate(date);
                            adClickTrend.setHour(hour);
                            adClickTrend.setMinute(minute);
                            adClickTrend.setAddid(adid);
                            adClickTrend.setClickCount(clickCount);

                            al.add(adClickTrend);
                        }
                        IAdClickTrendDAO iAdClickTrendDAO = DaoFactory.getIAdClickTrendDAO();
                        iAdClickTrendDAO.updateBatch(al);

                    }
                });
                return null;
            }
        });

    }


    private static JavaPairDStream<String, String> filterByBlacklist(
            JavaPairInputDStream<String, String> adRealTimeLogDStream) {
        // 刚刚接受到原始的用户点击行为日志之后
        // 根据mysql中的动态黑名单，进行实时的黑名单过滤（黑名单用户的点击行为，直接过滤掉，不要了）
        // 使用transform算子（将dstream中的每个batch RDD进行处理，转换为任意的其他RDD，功能很强大）

        JavaPairDStream<String, String> filteredAdRealTimeLogDStream = adRealTimeLogDStream.transformToPair(

                new Function<JavaPairRDD<String, String>, JavaPairRDD<String, String>>() {

                    private static final long serialVersionUID = 1L;

                    @SuppressWarnings("resource")
                    @Override
                    public JavaPairRDD<String, String> call(
                            JavaPairRDD<String, String> rdd) throws Exception {

                        // 首先，从mysql中查询所有黑名单用户，将其转换为一个rdd
                        IAdBlacklistDAO adBlacklistDAO = DaoFactory.getAdBlacklistDAO();
                        List<AdBlacklist> adBlacklists = adBlacklistDAO.findAll();

                        List<Tuple2<Long, Boolean>> tuples = new ArrayList<Tuple2<Long, Boolean>>();

                        for (AdBlacklist adBlacklist : adBlacklists) {
                            tuples.add(new Tuple2<Long, Boolean>(adBlacklist.getUserid(), true));
                        }

                        JavaSparkContext sc = new JavaSparkContext(rdd.context());
                        JavaPairRDD<Long, Boolean> blacklistRDD = sc.parallelizePairs(tuples);

                        // 将原始数据rdd映射成<userid, tuple2<string, string>>
                        JavaPairRDD<Long, Tuple2<String, String>> mappedRDD = rdd.mapToPair(new PairFunction<Tuple2<String, String>, Long, Tuple2<String, String>>() {

                            private static final long serialVersionUID = 1L;

                            @Override
                            public Tuple2<Long, Tuple2<String, String>> call(
                                    Tuple2<String, String> tuple)
                                    throws Exception {
                                String log = tuple._2;
                                String[] logSplited = log.split(" ");
                                long userid = Long.valueOf(logSplited[3]);
                                return new Tuple2<Long, Tuple2<String, String>>(userid, tuple);
                            }

                        });

                        // 将原始日志数据rdd，与黑名单rdd，进行左外连接
                        // 如果说原始日志的userid，没有在对应的黑名单中，join不到，左外连接
                        // 用inner join，内连接，会导致数据丢失

                        JavaPairRDD<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> joinedRDD = mappedRDD.leftOuterJoin(blacklistRDD);

                        JavaPairRDD<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> filteredRDD = joinedRDD.filter(new Function<Tuple2<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>>, Boolean>() {
                            @Override
                            public Boolean call(Tuple2<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> tuple) throws Exception {
                                Optional<Boolean> optional = tuple._2._2;

                                // 如果这个值存在，那么说明原始日志中的userid，join到了某个黑名单用户
                                if (optional.isPresent() && optional.get()) {
                                    return false;
                                }

                                return true;
                            }
                        });

                        JavaPairRDD<String, String> resultRDD = filteredRDD.mapToPair(

                                new PairFunction<Tuple2<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>>, String, String>() {

                                    private static final long serialVersionUID = 1L;

                                    @Override
                                    public Tuple2<String, String> call(
                                            Tuple2<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> tuple)
                                            throws Exception {
                                        return tuple._2._1;
                                    }

                                });

                        return resultRDD;
                    }

                });

        return filteredAdRealTimeLogDStream;
    }


    private static void generateDynamicBlacklist(JavaPairDStream<String, String> directStream) {
        JavaPairDStream<String, Long> pairDStream = directStream.mapToPair(new PairFunction<Tuple2<String, String>,
                String, Long>() {
            @Override
            public Tuple2<String, Long> call(Tuple2<String, String> tuple2) throws Exception {
                String s = tuple2._1;
                System.out.println(s);
                String DStream = tuple2._2;
                String[] logSplited = DStream.split(" ");

                // 提取出日期（yyyyMMdd）、userid、adid
                String timestamp = logSplited[0];
                Date date = new Date(Long.valueOf(timestamp));
                String datekey = DateUtils.formatDateKey(date);

                long userid = Long.valueOf(logSplited[3]);
                long adid = Long.valueOf(logSplited[4]);

                String key = datekey + "_" + userid + "_" + adid;

                return new Tuple2<String, Long>(key, 1L);
            }
        });


        //汇总每个batch每天点击量
        JavaPairDStream<String, Long> dailyClick = pairDStream.reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long v1, Long v2) throws Exception {
                return v1 + v2;
            }
        });


        //插入到mysql到mysql中
        dailyClick.foreachRDD(new VoidFunction<JavaPairRDD<String, Long>>() {
            @Override
            public void call(JavaPairRDD<String, Long> rdd) throws Exception {
                rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Long>>>() {


                    @Override
                    public void call(Iterator<Tuple2<String, Long>> tuple2Iterator) throws Exception {
                        List<ClickCount> ClickCounts = new ArrayList<ClickCount>();

                        while (tuple2Iterator.hasNext()) {
                            Tuple2<String, Long> tuple2 = tuple2Iterator.next();
                            Long clickCount = tuple2._2;
                            String[] strs = tuple2._1.split(" ");
                            String date = DateUtils.formatDate(DateUtils.parseDateKey(strs[0]));
                            // yyyy-MM-dd
                            long userid = Long.valueOf(strs[1]);
                            long adid = Long.valueOf(strs[2]);

                            ClickCount clickC = new ClickCount();
                            clickC.setAdid(adid);
                            clickC.setUserid(userid);
                            clickC.setClickCount(clickCount);
                            clickC.setDate(date);

                            ClickCounts.add(clickC);
                        }

                        ClickCountDao clickDao = DaoFactory.getClickDao();
                        clickDao.updateBatch(ClickCounts);
                    }
                });
            }
        });

        JavaPairDStream<String, Long> filterRdd = dailyClick.filter(new Function<Tuple2<String, Long>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Long> tuple) throws Exception {
                String key = tuple._1;
                String[] keySplited = key.split("_");

                // yyyyMMdd -> yyyy-MM-dd
                String date = DateUtils.formatDate(DateUtils.parseDateKey(keySplited[0]));
                long userid = Long.valueOf(keySplited[1]);
                long adid = Long.valueOf(keySplited[2]);

                // 从mysql中查询指定日期指定用户对指定广告的点击量
                ClickCountDao adUserClickCountDAO = DaoFactory.getClickDao();
                int clickCount = adUserClickCountDAO.findClickCountByMultiKey(
                        date, userid, adid);

                // 判断，如果点击量大于等于100，ok，那么不好意思，你就是黑名单用户
                // 那么就拉入黑名单，返回true
                if (clickCount >= 100) {
                    return true;
                }

                // 反之，如果点击量小于100的，那么就暂时不要管它了
                return false;
            }
        });
        // yyyyMMdd_userid_adid
        JavaDStream<Long> map = filterRdd.map(new Function<Tuple2<String, Long>, Long>() {
            @Override
            public Long call(Tuple2<String, Long> v1) throws Exception {
                String Str = v1._1;
                String[] strs = Str.split("_");
                Long userid = Long.valueOf(strs[1]);
                return userid;

            }
        });


        JavaDStream<Long> distinctBlacklistUseridDStream = map.transform(new Function<JavaRDD<Long>, JavaRDD<Long>>() {
            private static final long serialVersionUID = 1L;

            @Override
            public JavaRDD<Long> call(JavaRDD<Long> rdd) throws Exception {
                return rdd.distinct();
            }
        });

        distinctBlacklistUseridDStream.foreach(new Function<JavaRDD<Long>, Void>() {
            @Override
            public Void call(JavaRDD<Long> rdd) throws Exception {
                rdd.foreachPartition(new VoidFunction<Iterator<Long>>() {
                    @Override
                    public void call(Iterator<Long> iterator) throws Exception {
                        List<AdBlacklist> al = new ArrayList<AdBlacklist>();
                        while (iterator.hasNext()) {
                            Long userid = iterator.next();
                            AdBlacklist adBlacklist = new AdBlacklist();
                            adBlacklist.setUserid(userid);
                            al.add(adBlacklist);
                        }
                        IAdBlacklistDAO adBlacklistDAO = DaoFactory.getAdBlacklistDAO();
                        adBlacklistDAO.insertBatch(al);

                    }
                });
                return null;
            }
        });

    }

}
