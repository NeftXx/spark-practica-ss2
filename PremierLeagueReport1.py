from pyspark import SparkContext
import plotly.graph_objects as go


if __name__ == '__main__':
    input_file = "inputs/premier_league.csv"
    sc = SparkContext("local[3]", "word count")
    sc.setLogLevel("ERROR")
    season = "2017-2018"
    team = "Manchester United"

    rdd = sc.textFile(input_file).flatMap(lambda line: line.split('\n')) \
        .map(lambda line: line.split(','))

    rdd_list = rdd.collect()
    manchester_united_data = []
    for row_rdd in rdd_list:
        if row_rdd[5] == season and (row_rdd[0] == team or row_rdd[1] == team):
            manchester_united_data.append(row_rdd)
    rdd = sc.parallelize(manchester_united_data)

    results = ["H", "A", "D"]
    totals_list = []
    for header in results:
        row_list = rdd.filter(lambda item: header in item).collect()
        totals_list.append(len(row_list))
    results[0] = "Local"
    results[1] = "Visitante"
    results[2] = "Empate"
    figureData = [go.Pie(
        labels=results,
        values=totals_list
    )]
    figure = go.Figure(data=figureData)
    figure.update_layout(
        title="Resultados de cada partido del equipo Manchester United en la temporada 2017-2018",
        font=dict(
            family="Courier New, monospace",
            size=14,
            color="#7f7f7f"
        )
    )
    figure.show()
