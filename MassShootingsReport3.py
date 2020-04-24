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

    year_list = [2015]
    fatalities_list = []
    fatalities = 0
    injured_list = []
    injured = 0
    total_victims_list = []
    total_victims = 0
    for year in year_list:
        rdd_list = rdd.filter(lambda x: datetime.strptime(x[3], '%m/%d/%Y').year == year).collect()
        for row in rdd_list:
            fatalities = fatalities + float(row[5])
            injured = injured + float(row[6])
            total_victims = total_victims + float(row[7])
        fatalities_list.append(fatalities)
        injured_list.append(injured)
        total_victims_list.append(total_victims)
        fatalities = 0
        injured = 0
        total_victims = 0

    figureData = [
        go.Bar(name='Muertes', x=year_list, y=fatalities_list),
        go.Bar(name='Heridos', x=year_list, y=injured_list),
        go.Bar(name='Total de víctimas', x=year_list, y=total_victims_list)
    ]
    figure = go.Figure(data=figureData)
    figure.update_layout(
        title="Datos de tiroteos del año 2015",
        barmode='group',
        font=dict(
            family="Courier New, monospace",
            size=14,
            color="#7f7f7f"
        )
    )
    figure.show()
