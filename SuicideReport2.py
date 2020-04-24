import re
from pyspark import SparkContext
import plotly.graph_objects as go


if __name__ == '__main__':
    input_file = "inputs/suicide.csv"
    country = "Guatemala"
    sc = SparkContext("local[3]", "word count")
    sc.setLogLevel("ERROR")
    rdd = sc.textFile(input_file) \
        .flatMap(lambda line: line.split('\n'))\
        .map(lambda line: (re.sub(r'\"(\d+),(\d+),(\d+),(\d+)\"', r'\1\2\3\4', line))
             .split(','))

    rdd_list = rdd.collect()
    age_list = []
    for item_rdd in rdd_list:
        if item_rdd[0] == country:
            age_list.append(item_rdd[3])
    age_list.pop(0)
    age_list = sc.parallelize(age_list).distinct().collect()

    totals_list = []
    total_suicides = 0
    for age in age_list:
        row_list = rdd.filter(lambda item: age in item).collect()
        for row in row_list:
            if row[0] == country:
                total_suicides = total_suicides + float(row[4])
        totals_list.append(total_suicides)
        total_suicides = 0

    figureData = [go.Pie(
        labels=age_list,
        values=totals_list
    )]
    figure = go.Figure(data=figureData)
    figure.update_layout(
        title="Total de suicidios por edad en " + country,
        font=dict(
            family="Courier New, monospace",
            size=14,
            color="#7f7f7f"
        )
    )
    figure.show()
