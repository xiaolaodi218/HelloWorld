import pandas as pd
import numpy as np
import scipy.stats
from scipy.stats import skew, boxcox


from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import DataFrameStatFunctions
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Desc").getOrCreate()
spark.sparkContext.setLogLevel('ERROR')


def boxcox_transform(df, column, cutoff):
    skew_value = skew(df[column])
    if skew(df[column]) > cutoff:
        df["boxcoxTransform"] = boxcox(df[column])[0]
        mean_value = df["boxcoxTransform"].mean()
        std_value = df["boxcoxTransform"].std(ddof=0)
        lambda_value = boxcox(df[column])[1]
        df["boxcoxTransform"] = (df["boxcoxTransform"] - mean_value) / std_value
        df["score"] = df["boxcoxTransform"].apply(lambda x: scipy.stats.norm(0, 1).cdf(x))
    else:
        mean_value = df[column].mean()
        std_value = df[column].std(ddof=0)
        df[column] = (df[column] - mean_value) / std_value
        df["score"] = df[column].apply(lambda x: scipy.stats.norm(0, 1).cdf(x))
    return df, mean_value, std_value, lambda_value ,skew_value


# boxcox(x, lmbda=None, alpha=None)
def boxcox_transform_newdata(df,column,skew_value,cutoff,mean_value,lambda_value,std_value):
    if skew_value> cutoff:
        df['boxcoxTransform'] = df[column].apply(lambda x : ( x**lambda_value - 1) / lambda_value if lambda_value>0  else np.log(x) )
        df["boxcoxTransform"] = (df["boxcoxTransform"] - mean_value) / std_value
        df["score"] = df["boxcoxTransform"].apply(lambda x: scipy.stats.norm(0, 1).cdf(x))
    else:
        df["boxcoxTransform"] = (df[column] - mean_value) / std_value
        df["score"] = df["boxcoxTransform"].apply(lambda x: scipy.stats.norm(0, 1).cdf(x))
    return df

 
if __name__ == "__main__": 
    #read from hive
    
    #survey data to compute lambda
    resultsBeliefPropagation_1207_pd_survey = spark.sql("select * from tmp.resultsBeliefPropagation where attr>0 order by rand() limit 500000 ").toPandas()
    #month 7 to score 
    raw_score_BeliefPropagation_7 = spark.sql("select * from tmp.raw_score_7_1207 where attr>0 ").toPandas()
    
    #compute score
    raw_score_BeliefPropagation_7_scored_pd_survey , mean_value, std_value, lambda_value, skew_value = boxcox_transform(resultsBeliefPropagation_1207_pd_survey, "attr", 0.25)
    print "lambda_value :" + str(lambda_value)
    print "mean_value :" + str(mean_value)
    print "std_value :" + str(std_value)
    print "skew_value :" + str(skew_value)
    
    print raw_score_BeliefPropagation_7_scored_pd_survey.head(20)
    print skew(raw_score_BeliefPropagation_7_scored_pd_survey['boxcoxTransform'])
      
    raw_score_BeliefPropagation_7_scored_pd = boxcox_transform_newdata(raw_score_BeliefPropagation_7,"attr",skew_value,0.25,mean_value,lambda_value,std_value)
    
    # write to hive
    print "write to hive"
    raw_score_BeliefPropagation_7_scored = spark.createDataFrame(raw_score_BeliefPropagation_7_scored_pd)
    raw_score_BeliefPropagation_7_scored.write.mode("overwrite").saveAsTable("tmp.raw_score_BeliefPropagation_7_scored")


