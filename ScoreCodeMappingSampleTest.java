package com.spgmi.ca.benchmark.paysense;

import static org.apache.spark.sql.functions.*;


import java.io.IOException;
import java.io.Serializable;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.spgmi.ca.benchmark.config.ConsumerConfig;
import com.spgmi.ca.benchmark.config.SparkConfig;
import com.spgmi.ca.benchmark.service.PaysesneBenchmarkService;
import com.spgmi.ca.benchmark.spark.BmBaseProcessor;
import com.spgmi.ca.benchmark.utils.BenchmarkConstants;
import com.spgmi.ca.benchmark.utils.BenchmarkUtils;

import javassist.bytecode.stackmap.TypeTag;
import scala.Tuple2;


class InputBean implements Serializable {

	private static final long serialVersionUID = -8031530478197586372L;
	
	public InputBean() {
		// TODO Auto-generated constructor stub
	}
	public Integer getCode1() {
		return code1;
	}
	public void setCode1(Integer code1) {
		this.code1 = code1;
	}
	public Integer getCode2() {
		return code2;
	}
	public void setCode2(Integer code2) {
		this.code2 = code2;
	}
	public Double getCode3() {
		return code3;
	}
	public void setCode3(Double code3) {
		this.code3 = code3;
	}
	private Integer code1;
	private Integer code2;
	private Double code3;
	
	
	
}


class OutputBean implements Serializable {

	private static final long serialVersionUID = -8031530478197586372L;
	
	public OutputBean() {
		// TODO Auto-generated constructor stub
	}
	public Integer getCode1() {
		return code1;
	}
	public void setCode1(Integer code1) {
		this.code1 = code1;
	}
	public Integer getCode2() {
		return code2;
	}
	public void setCode2(Integer code2) {
		this.code2 = code2;
	}
	public Double getCode3() {
		return code3;
	}
	public void setCode3(Double code3) {
		this.code3 = code3;
	}
	private Integer code1;
	private Integer code2;
	private Double code3;
	
	
	
}

class LookupMapClass extends HashMap<Integer,Integer> implements  TypeTag {
	
}

public class ScoreCodeMappingSampleTest extends BmBaseProcessor{
	

	/*private static final List<String> BM_MANDATORY_COLS_LST = Arrays.asList("data_date","company_id", "country_iso_code", "currency_code","industry_classification_code", "total_revenue","model_id", "create_date", "last_update_date", "version_id");
    private static final List<String> PAYSENSE_DATA_ITEM_CODES_LST = Arrays.asList("payrate_within_terms_3m_latest", "payrate_0_30_3m_latest", "payrate_31_60_3m_latest", "payrate_61_90_3m_latest","predicted_payment_rate_within_terms",  "predicted_payrate_0_30_3m_latest",  "predicted_payrate_31_60_3m_latest",  "predicted_payrate_61_90_3m_latest");
	*/
	protected ScoreCodeMappingSampleTest(String[] args) {
	      super(args);
	}

    public static void main(String[] args) {
    	ScoreCodeMappingSampleTest bmProcessor = new ScoreCodeMappingSampleTest(args);
    	bmProcessor.startSparkJob();
    }

    @Override
    protected void triggerBmProcessor(ConsumerConfig consumerConfig, SparkConfig sparkConfig, SparkSession sparkSession) throws SQLException, IndexOutOfBoundsException, IOException, ParseException {
       
    	/*List<StructField> fields = Arrays.asList(
    			  DataTypes.createStructField("line", DataTypes.StringType, true));
    			StructType schema = DataTypes.createStructType(fields);
    			DataFrame df = sqlContext.createDataFrame(rowRDD, schema);*/
    	
    	//Dataset<Row> df = createSampleDataset(spark);
    	
       //Broadcast<Map<Integer,Integer>> getDataItemScoreDelayMap(SparkSession spark,Dataset<Row> lookUpDs)
    	
    	/*Dataset<Row> scoreDelaysDs = BenchmarkUtils.getPaymentScoreDelayMapping(sparkSession ,rdsConfig)
			    			.withColumn("mapping_code", col("mapping_code").cast(DataTypes.IntegerType))
			    			.withColumn("mapping_value", col("mapping_value").cast(DataTypes.IntegerType))
			    			;
    	
    	//scoreDelaysDs.show(2);
    	//scoreDelaysDs.printSchema();
    	//mapping_code,mapping_value  
    	
    	Broadcast<Map<Integer, Integer>> lookUpScoreDelayMapBcVar  = PaysesneBenchmarkService.getDataItemScoreDelayMap(sparkSession, scoreDelaysDs);
    	lookUpScoreDelayBcMap = lookUpScoreDelayMapBcVar;
    	sparkSession.udf().register("lookUpScoreDelayMap", lookUpScoreDelayMapUDF, DataTypes.IntegerType);*/
    	
     /*final Map<Column,Integer> lookUpScoreHm = new HashMap<>();
		      lookUpScoreHm.put(lit(1),11);
		      lookUpScoreHm.put(lit(2),21);
		      lookUpScoreHm.put(lit(3),31);
		      lookUpScoreHm.put(lit(4),41);
		      lookUpScoreHm.put(lit(5),51);*/
    	
    	// final LookupMapClass lookUpScoreHm = new LookupMapClass();
    	 final Map<Integer,Integer> lookUpScoreHm = new HashMap<>();
	      lookUpScoreHm.put(1,11);
	      lookUpScoreHm.put(2,21);
	      lookUpScoreHm.put(3,31);
	      lookUpScoreHm.put(4,41);
	      lookUpScoreHm.put(5,51);
	      
      
      
      MapType mapSchema = DataTypes.createMapType(DataTypes.IntegerType, DataTypes.IntegerType);
        
        JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
      Broadcast<Map<Integer,Integer>> lookUpScoreDelayBcVar = javaSparkContext.broadcast(lookUpScoreHm);
        
    	
    	
        List<String[]> stringAsList = new ArrayList<>();
        stringAsList.add(new String[] { "1","7","5.0" });
		stringAsList.add(new String[] { "2","7","4.0" });
		stringAsList.add(new String[] { "3","7","3.0" });
		stringAsList.add(new String[] { "4","7","2.0" });
		stringAsList.add(new String[] { "5","7","1.0" });
        
        JavaSparkContext sparkContext = new JavaSparkContext(sparkSession.sparkContext());
        JavaRDD<Row> rowRDD = sparkContext.parallelize(stringAsList).map((String[] row) -> RowFactory.create(row));

       
        StructType schema = DataTypes
                .createStructType(new StructField[] {
                        DataTypes.createStructField("code1", DataTypes.StringType, false),
                        DataTypes.createStructField("code2", DataTypes.StringType, false),
                        DataTypes.createStructField("code3", DataTypes.StringType, false)
                      
                });

        Dataset<Row> dataDf= sparkSession.sqlContext().createDataFrame(rowRDD, schema).toDF();

		Dataset<Row> buckettedConstituentsDs = dataDf
		                .withColumn("code1", col("code1").cast(DataTypes.IntegerType))
		                .withColumn("code2", col("code2").cast(DataTypes.IntegerType))
		                .withColumn("code3", col("code3").cast(DataTypes.DoubleType))
		                  ;
    	
    	//Dataset<Row> buckettedConstituentsDs = applyProperDataTypes(df);
        
    	buckettedConstituentsDs.show(false);
    	
    	/*StructType dataSchema = buckettedConstituentsDs.schema();
    	
    	StructType ndataSchema = dataSchema.add("lookup_map" , mapSchema);*/
    	
    	
    	/*Dataset<Row> delaysDs = PaysesneBenchmarkService.copyAndAddPaymentDelayRows(buckettedConstituentsDs);
    	delaysDs.show(false);
    	delaysDs.printSchema();*/
    	
    	
    /*	Dataset<Row> delaysDs2 =  delaysDs
    	    .withColumn("floor_mean", floor(col("mean")))
    	    .withColumn("floor_percentile_75", floor(col("percentile_75")))
    	    .withColumn("round_mean", round(col("mean"),0))
    	    .withColumn("round_percentile_75", round(col("percentile_75"),0))
    	    //.withColumn("percentile_75", abs(col("percentile_75").cast(DataTypes.StringType))
    	    //.show(false)
    	    ;
        
    	delaysDs2.show();
    	delaysDs2.printSchema();*/
    	
    	//Dataset<Row> mappedScoreDelaysDs = PaysesneBenchmarkService.applyRoundingAndMappingToBMs(delaysDs,  scoreDelayMapVar.getValue() )
    	
    	sparkSession.udf().register("lookUpScoreDelayMap", lookUpScoreDelayMapUDF2, DataTypes.IntegerType);
    	
    	/*Dataset<Row> delaysDs = buckettedConstituentsDs
						            .withColumn("code3",  functions.callUDF("lookUpScoreDelayMap",lit(lookUpScoreDelayBcVar.getValue()),floor(col("code3")).cast(DataTypes.IntegerType))
								      )
    			.withColumn("floor_code3", floor(col("code3"))  
					      )
    			.withColumn("floor_code3_int", floor(col("code3")).cast(DataTypes.IntegerType)  
					      )
    			.withColumn("five", lit(((Map<Integer, Integer>)lookUpScoreDelayBcVar.getValue()).get(5))  
					      )
	            .withColumn("map_code3", lit(((Map<Integer, Integer>)lookUpScoreDelayBcVar.getValue()).get(col("floor_code3_int")))  
					      )
	            
	            .withColumn("five_lit", lit(((Map<Integer, Integer>)lookUpScoreDelayBcVar.getValue()).get(lit(5).cast(DataTypes.IntegerType))) 
					      )
	            ;*/
    	
    	
    	//Encoder<Tuple2<Integer, Integer>> encoder2 = Encoders.tuple(Encoders.INT(), Encoders.INT());
    	
    	Map<Integer, Integer> tt;
    	
    	/*
    	 The method typedLit(T, TypeTags.TypeTag<T>) in the type functions is not applicable for the arguments (String, LookupMapClass)
    	 */
    	
    	
    	Column typedMap= functions.typedLit(lookUpScoreHm,LookupMapClass);
    	//Map<Encoders.INT(), Encoders.INT()> 
    	//Map<Encoders.INT(), Encoders.INT()> );
    	
    	Dataset<Row> delaysDs = buckettedConstituentsDs
	            /*.withColumn("code3",  functions.callUDF("lookUpScoreDelayMap",lit(lookUpScoreDelayBcVar.getValue()),floor(col("code3")).cast(DataTypes.IntegerType))
			      )*/
				.withColumn("floor_code3", floor(col("code3"))  
				      )
				.withColumn("floor_code3_int", floor(col("code3")).cast(DataTypes.IntegerType)  
				      )
				.withColumn("five", lit(((Map<Integer, Integer>)lookUpScoreDelayBcVar.getValue()).get(5))  
				      )
				.withColumn("map_code3", lit(((Map<Integer, Integer>)lookUpScoreDelayBcVar.getValue()).get(lit(col("floor_code3_int"))))  
				      )
				
				.withColumn("five_lit", lit(((Map<Integer, Integer>)lookUpScoreDelayBcVar.getValue()).get(lit(5))) 
				      )
				;
    	
    	/*Dataset<Row> delaysDs = buckettedConstituentsDs
    			.withColumn("code3",  functions.callUDF("lookUpScoreDelayMap",lookUpScoreDelayBcVar,floor(col("code3")).cast(DataTypes.IntegerType))
					      );*/
			    
    	delaysDs.printSchema();
    	delaysDs.show(10);
    	
    	
        System.out.println("Done");
    

	}
    
    public static UDF1 lookUpScoreDelayMapUDF1 = new UDF1<Integer, Integer>() {
		private static final long serialVersionUID = 1L;
		
	    @Override
		public Integer call(Integer score) throws Exception {
			//Map<Integer, Integer> bclookUpHm = lookUpScoreDelayMapBcVar.getValue();
	    	
	    	 //java.util.Map<Integer, Integer> lookUpScoreHm = getConstructedScoreMap();
			return null;//bclookUpHm.get(score);
		}

		private java.util.Map<Integer, Integer> getConstructedScoreMap() {
			java.util.Map<Integer,Integer> lookUpScoreHm = new java.util.HashMap<>();
	         /*lookUpScoreHm.put(1,11);
	         lookUpScoreHm.put(2,21);
	         lookUpScoreHm.put(3,31);
	         lookUpScoreHm.put(4,41);
	         lookUpScoreHm.put(5,51);*/
			lookUpScoreHm.put(100,0);
			lookUpScoreHm.put(99,1);
			lookUpScoreHm.put(98,2);
			lookUpScoreHm.put(97,2);
			lookUpScoreHm.put(96,3);
			lookUpScoreHm.put(95,4);
			lookUpScoreHm.put(94,5);
			lookUpScoreHm.put(93,5);
			lookUpScoreHm.put(92,6);
			lookUpScoreHm.put(91,7);
			lookUpScoreHm.put(90,8);
			lookUpScoreHm.put(89,8);
			lookUpScoreHm.put(88,9);
			lookUpScoreHm.put(87,10);
			lookUpScoreHm.put(86,11);
			lookUpScoreHm.put(85,11);
			lookUpScoreHm.put(84,12);
			lookUpScoreHm.put(83,13);
			lookUpScoreHm.put(82,14);
			lookUpScoreHm.put(81,14);
			lookUpScoreHm.put(80,15);
			lookUpScoreHm.put(79,16);
			lookUpScoreHm.put(78,17);
			lookUpScoreHm.put(77,17);
			lookUpScoreHm.put(76,18);
			lookUpScoreHm.put(75,19);
			lookUpScoreHm.put(74,20);
			lookUpScoreHm.put(73,20);
			lookUpScoreHm.put(72,21);
			lookUpScoreHm.put(71,22);
			lookUpScoreHm.put(70,23);
			lookUpScoreHm.put(69,23);
			lookUpScoreHm.put(68,24);
			lookUpScoreHm.put(67,25);
			lookUpScoreHm.put(66,26);
			lookUpScoreHm.put(65,26);
			lookUpScoreHm.put(64,27);
			lookUpScoreHm.put(63,28);
			lookUpScoreHm.put(62,29);
			lookUpScoreHm.put(61,29);
			lookUpScoreHm.put(60,30);
			lookUpScoreHm.put(59,32);
			lookUpScoreHm.put(58,34);
			lookUpScoreHm.put(57,36);
			lookUpScoreHm.put(56,38);
			lookUpScoreHm.put(55,40);
			lookUpScoreHm.put(54,42);
			lookUpScoreHm.put(53,44);
			lookUpScoreHm.put(52,46);
			lookUpScoreHm.put(51,48);
			lookUpScoreHm.put(50,50);
			lookUpScoreHm.put(49,52);
			lookUpScoreHm.put(48,54);
			lookUpScoreHm.put(47,56);
			lookUpScoreHm.put(46,58);
			lookUpScoreHm.put(45,60);
			lookUpScoreHm.put(44,62);
			lookUpScoreHm.put(43,64);
			lookUpScoreHm.put(42,66);
			lookUpScoreHm.put(41,68);
			lookUpScoreHm.put(40,70);
			lookUpScoreHm.put(39,72);
			lookUpScoreHm.put(38,74);
			lookUpScoreHm.put(37,76);
			lookUpScoreHm.put(36,78);
			lookUpScoreHm.put(35,80);
			lookUpScoreHm.put(34,82);
			lookUpScoreHm.put(33,84);
			lookUpScoreHm.put(32,86);
			lookUpScoreHm.put(31,88);
			lookUpScoreHm.put(30,90);
			lookUpScoreHm.put(29,91);
			lookUpScoreHm.put(28,91);
			lookUpScoreHm.put(27,92);
			lookUpScoreHm.put(26,92);
			lookUpScoreHm.put(25,93);
			lookUpScoreHm.put(24,93);
			lookUpScoreHm.put(23,94);
			lookUpScoreHm.put(22,94);
			lookUpScoreHm.put(21,95);
			lookUpScoreHm.put(20,95);
			lookUpScoreHm.put(19,96);
			lookUpScoreHm.put(18,96);
			lookUpScoreHm.put(17,97);
			lookUpScoreHm.put(16,97);
			lookUpScoreHm.put(15,98);
			lookUpScoreHm.put(14,98);
			lookUpScoreHm.put(13,99);
			lookUpScoreHm.put(12,99);
			lookUpScoreHm.put(11,100);
			lookUpScoreHm.put(10,100);
			lookUpScoreHm.put(9,101);
			lookUpScoreHm.put(8,101);
			lookUpScoreHm.put(7,102);
			lookUpScoreHm.put(6,102);
			lookUpScoreHm.put(5,103);
			lookUpScoreHm.put(4,103);
			lookUpScoreHm.put(3,104);
			lookUpScoreHm.put(2,104);
			lookUpScoreHm.put(1,105);
			return lookUpScoreHm;
		}

    };
    
    public static UDF2 lookUpScoreDelayMapUDF2 = new UDF2< Broadcast<Map<Integer,Integer>> ,Integer, Integer>() {
		private static final long serialVersionUID = 1L;
		
	    @Override
		public Integer call( Broadcast<Map<Integer,Integer>> lookUpScoreDelayBcVar, Integer score) throws Exception {
	    	//System.out.println("keys --> :" + bclookUpHm.keySet().toString());
	    	
	    	Map<Integer, Integer> bclookUpHm  = lookUpScoreDelayBcVar.getValue();
			return bclookUpHm.get(score);
		}
    };
    
    
    
    /*
       select countryisocode,dollarweighted_trade_payment_score,reported_date,cs_company_id,bankruptcy_score_value,
 conditional_payment_1_days_past_due_trade_payment_score, conditional_payment_31_days_past_due_trade_payment_score, conditional_payment_61_days_past_due_trade_payment_score, conditional_payment_91_days_past_due_trade_payment_score, 
 conditional_payment_1_days_past_due_exp_days_of_delay, conditional_payment_31_days_past_due_exp_days_of_delay, conditional_payment_61_days_past_due_exp_days_of_delay, conditional_payment_91_days_past_due_exp_days_of_delay, 
 unconditional_payment_exp_days_of_delay, unconditional_trade_payment_score
 ,cs_company_id,reported_date,model_id,create_date,last_update_date
     */
    
    /*
     when( col("data_item_code").isInCollection(bankruptcy_delay_cols) ,
			                        when(  (col("percentile_0").isin(0.0).or(col("percentile_10").isin(0.0)).or(col("percentile_25").isin(0.0)).or(col("percentile_50").isin(0.0)).or(col("percentile_75").isin(0.0)).or(col("percentile_100").isin(0.0))
			                                 .or(col("mean").isin(0.0))
			                               )
			                                 .and( array_contains(col("bankruptcy_score_value_lst"), "sd").or( array_contains(col("bankruptcy_score_value_lst"), "d")) )  , 
			                            when( array_contains(col("bankruptcy_score_value_lst"), "sd")  ,  lit("sd")).otherwise(lit("d"))
			                          )
			                         .otherwise(lit(null))
			                    )
			                  .otherwise(col("mean"))
     */
    
    
    

	/**
	 * @param df
	 * @return
	 */
	private Dataset<Row> applyProperDataTypes(Dataset<Row> df) {
		Dataset<Row> buckettedConstituentsDs = df
			      .withColumn("model_family_id", col("model_family_id").cast(DataTypes.IntegerType))
			      .withColumn("classification_type", col("classification_type").cast(DataTypes.StringType))
			      .withColumn("classification_value", col("classification_value").cast(DataTypes.StringType))
			      .withColumn("data_date", to_date(col("data_date"),"yyyy-MM-dd").cast(DataTypes.DateType))
			      //.withColumn("country_iso_code", col("country_iso_code").cast(DataTypes.StringType))
			       .withColumn("mean", col("mean").cast(DataTypes.StringType))
			       .withColumn("obs_cnt", col("obs_cnt").cast(DataTypes.LongType))
			      .withColumn("percentile_0", col("percentile_0").cast(DataTypes.StringType))
				  .withColumn("percentile_10", col("percentile_10").cast(DataTypes.StringType))
				  .withColumn("percentile_25", col("percentile_25").cast(DataTypes.StringType))
				  .withColumn("percentile_50", col("percentile_50").cast(DataTypes.StringType))
				  .withColumn("percentile_75", col("percentile_75").cast(DataTypes.StringType))
				  .withColumn("percentile_90", col("percentile_90").cast(DataTypes.StringType))
				  .withColumn("percentile_100", col("percentile_100").cast(DataTypes.StringType))
				  .withColumn("bankruptcy_constituent_indicator", col("bankruptcy_constituent_indicator").cast(DataTypes.StringType))
				  
			     .withColumn("data_item_code",  col("data_item_code").cast(DataTypes.StringType))
			     .withColumn("model_family_id", col("model_family_id").cast(DataTypes.IntegerType))
			     .withColumn("create_date", to_timestamp( col("create_date"),"yyyy-MM-dd HH:mm").cast(DataTypes.TimestampType))
			     
			     .withColumn("version_id", col("version_id").cast(DataTypes.IntegerType))
				 .withColumn("reported_year", col("reported_year").cast(DataTypes.IntegerType))
				 .withColumn("reported_month", col("reported_month").cast(DataTypes.IntegerType));
		
		
		return buckettedConstituentsDs;
	}

	/**
	 * @param spark
	 * @return
	 */
	private Dataset<Row> createBenchmarkDatasetFromList(SparkSession spark) {
		List<String[]> stringAsList = new ArrayList<>();
        stringAsList.add(new String[] { "7", "COUNTRY", "conditional_payment_1_days_past_due_trade_payment_score",  "2020-06-01","USA",
        		"sd","2020-07-28 06:31:48","33.142857142857146","7", "0.0","0.0","0.0","0.0","76.0","80.0","80.0","1","2018","3" });
       /* stringAsList.add(new String[] { "7", "COUNTRY", "conditional_payment_1_days_past_due_trade_payment_score",  "2020-06-01","USA",
        		"d","2020-07-28 06:31:48","33.142857142857146","7","bb-","bb-","bb-","bb-","bb-","bb-","bb-", "1","2018","3" });*/
        
        
     /*   (create_date,mean,obs_cnt,percentile_0,percentile_10,percentile_100,percentile_25,percentile_50,percentile_75,percentile_90,reported_month,reported_year,version_id) 
        VALUES 33.142857142857146,7,0.0,0.0,80.0,0.0,0.0,76.0,80.0,6,2020,1);*/


        JavaSparkContext sparkContext = new JavaSparkContext(spark.sparkContext());

        JavaRDD<Row> rowRDD = sparkContext.parallelize(stringAsList).map((String[] row) -> RowFactory.create(row));

        // Creates schema
        StructType schema = DataTypes
                .createStructType(new StructField[] {
                		DataTypes.createStructField("model_family_id", DataTypes.StringType, false),
                		DataTypes.createStructField("classification_type", DataTypes.StringType, false),
                		DataTypes.createStructField("data_item_code", DataTypes.StringType, false),
                		DataTypes.createStructField("data_date", DataTypes.StringType, false),
                		DataTypes.createStructField("classification_value", DataTypes.StringType, false),
                		
                		DataTypes.createStructField("bankruptcy_constituent_indicator", DataTypes.StringType, false),
                		DataTypes.createStructField("create_date", DataTypes.StringType, false),
                		
                		//DataTypes.createStructField("country_iso_code", DataTypes.StringType, false),
                		DataTypes.createStructField("mean", DataTypes.StringType, false),
                		DataTypes.createStructField("obs_cnt", DataTypes.StringType, false),
                		DataTypes.createStructField("percentile_0", DataTypes.StringType, false),
                		DataTypes.createStructField("percentile_10", DataTypes.StringType, false),
                		DataTypes.createStructField("percentile_25", DataTypes.StringType, false),
                		DataTypes.createStructField("percentile_50", DataTypes.StringType, false),
                		DataTypes.createStructField("percentile_75", DataTypes.StringType, false),
                		
                		DataTypes.createStructField("percentile_90", DataTypes.StringType, false),
                		DataTypes.createStructField("percentile_100", DataTypes.StringType, false),
                		//DataTypes.createStructField("model_family_id", DataTypes.StringType, false),
                		
                		
                		DataTypes.createStructField("version_id", DataTypes.StringType, false), 
                		DataTypes.createStructField("reported_year", DataTypes.StringType, false), 
                		DataTypes.createStructField("reported_month", DataTypes.StringType, false), 
                		
                        
                        
       			      //.withColumn("data_date", to_date(col("data_date"),"yyyy-MM-dd").cast(DataTypes.DateType))
       			     //.withColumn("create_date", to_timestamp( col("create_date"),"yyyy-MM-dd HH:mm").cast(DataTypes.TimestampType))
                });

        Dataset<Row> df = spark.sqlContext().createDataFrame(rowRDD, schema).toDF();
		return df;
		
 			/*val df = data.toDF("model_id", "classification_type", "classification_value", "data_date","country_iso_code","mean","obs_cnt",
 			                   "percentile_0","percentile_10","percentile_25","percentile_50","percentile_75","percentile_90","percentile_100",
 			                   "data_item_code","model_family_id","create_date","version_id","reported_year","reported_month")*/



	}
	
	/**
	 * @param spark
	 * @return
	 */
	private Dataset<Row> createSampleDataset(SparkSession spark) {
		List<String[]> stringAsList = new ArrayList<>();
        stringAsList.add(new String[] { "bar1.1", "bar2.1" });
        stringAsList.add(new String[] { "bar1.2", "bar2.2" });

        JavaSparkContext sparkContext = new JavaSparkContext(spark.sparkContext());

        JavaRDD<Row> rowRDD = sparkContext.parallelize(stringAsList).map((String[] row) -> RowFactory.create(row));

        // Creates schema
        StructType schema = DataTypes
                .createStructType(new StructField[] { DataTypes.createStructField("foe1", DataTypes.StringType, false),
                        DataTypes.createStructField("foe2", DataTypes.StringType, false) });

        Dataset<Row> df = spark.sqlContext().createDataFrame(rowRDD, schema).toDF();
		return df;
	}
	

	@Override
	protected Dataset<Row> getConstituentsDataset(Dataset<Row> selectedCalcIntervalDs, String classificationType) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected Dataset<Row> getSelectedConstituentsColumns(Dataset<Row> ds) {
		// TODO Auto-generated method stub
		return null;
	}

}
