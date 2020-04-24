# coding=utf-8
from datetime import datetime
from pyspark import SparkContext
from pyspark.sql import SQLContext
import plotly.graph_objects as go


if __name__ == '__main__':
    input_file = "inputs/mass_shootings.csv"
    sc = SparkContext("local[3]", "word count")
    sc.setLogLevel("ERROR")
    sqlContext = SQLContext(sc)
    rdd = sqlContext.read.format("com.databricks.spark.csv") \
        .options(header="true", inferschema="true") \
        .load(input_file).rdd

    rdd_list = rdd.collect()
    year_list = []
    temp_rdd_list = []
    for row_rdd in rdd_list:
        year_list.append(datetime.strptime(row_rdd[3], '%m/%d/%Y').year)

    year_list = sc.parallelize(year_list).distinct().collect()

    fatalities_totals_list = []
    fatalities = 0
    for year in year_list:
        shooting_filter = rdd.filter(lambda x: datetime.strptime(x[3], '%m/%d/%Y').year == year).collect()
        for shooting in shooting_filter:
            fatalities = fatalities + float(shooting[5])
        fatalities_totals_list.append(fatalities)
        fatalities = 0

    rows = list(zip(year_list, fatalities_totals_list))
    year_with_more_fatalities = sc.parallelize(rows).takeOrdered(1, key=lambda x: -x[1])
    print(year_with_more_fatalities)

    figureData = [go.Bar(
        x=[year_with_more_fatalities[0][0]],
        y=[year_with_more_fatalities[0][1]]
    )]
    figure = go.Figure(data=figureData)
    figure.update_layout(
        title="Año con más muertes",
        xaxis_title="Año",
        yaxis_title="Total de muertes",
        font=dict(
            family="Courier New, monospace",
            size=14,
            color="#7f7f7f"
        )
    )
    figure.show()
