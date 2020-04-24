# coding=utf-8
import re
from pyspark import SparkContext
import plotly.graph_objects as go


if __name__ == '__main__':
    input_file = "inputs/suicide.csv"
    sc = SparkContext("local[3]", "word count")
    sc.setLogLevel("ERROR")
    rdd = sc.textFile(input_file)\
        .map(lambda line: (re.sub(r'\"(\d+),(\d+),(\d+),(\d+)\"', r'\1\2\3\4', line))
             .split(','))
    generations = ["Generation X", "Generation Z", "Boomers", "Silent", "G.I. Generation", "Millenials"]

    totals_list = []
    total_suicides = 0

    for generation in generations:
        generation_filter = rdd.filter(lambda item: generation in item).collect()
        for row in generation_filter:
            total_suicides = total_suicides + float(row[4])
        totals_list.append(total_suicides)
        total_suicides = 0

    figureData = [go.Bar(
        x=generations,
        y=totals_list
    )]
    figure = go.Figure(data=figureData)
    figure.update_layout(
        title="Total de suicidios por generación",
        xaxis_title="Generación",
        yaxis_title="Total de suicidios",
        xaxis={"categoryorder": "total descending"},
        font=dict(
            family="Courier New, monospace",
            size=14,
            color="#7f7f7f"
        )
    )
    figure.show()
