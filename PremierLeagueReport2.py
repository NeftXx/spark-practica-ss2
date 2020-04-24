from pyspark import SparkContext
import plotly.graph_objects as go


if __name__ == '__main__':
    input_file = "inputs/premier_league.csv"
    sc = SparkContext("local[3]", "word count")
    sc.setLogLevel("ERROR")
    season = "2012-2013"
    rdd = sc.textFile(input_file).flatMap(lambda line: line.split('\n')) \
        .map(lambda line: line.split(','))
    rdd_list = rdd.collect()

    season_data = []
    for row_rdd in rdd_list:
        if row_rdd[5] == season:
            season_data.append(row_rdd)
    rdd = sc.parallelize(season_data)

    away_teams = []
    for data in season_data:
        away_teams.append(data[1])

    away_teams = sc.parallelize(away_teams).distinct().collect()

    away_goals = []
    goals = 0
    for team in away_teams:
        row_list = rdd.filter(lambda item: team in item).collect()
        for row in row_list:
            goals = goals + float(row[3])
        away_goals.append(goals)
        goals = 0

    figureData = [go.Bar(
        x=away_teams,
        y=away_goals
    )]
    figure = go.Figure(figureData)
    figure.update_layout(
        title="Total de goles de visita",
        xaxis_title="Equipo visitante",
        yaxis_title="Total de goles",
        xaxis={"categoryorder": "total descending"},
        font=dict(
            family="Courier New, monospace",
            size=14,
            color="#7f7f7f"
        )
    )
    figure.show()
