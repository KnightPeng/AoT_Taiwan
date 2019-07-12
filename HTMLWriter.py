import datetime

import pandas as pd
from bs4 import BeautifulSoup


class HTMLWriter:
    html_str = """
    <html>
        <head>
            <title>AoT Check</title>
            <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0/css/bootstrap.min.css" integrity="sha384-Gn5384xqQ1aoWXA+058RXPxPg6fy4IWvTNh0E263XmFcJlSAwiGgFAW/dAiS6JXm" crossorigin="anonymous">
            <link href="https://unpkg.com/bootstrap-table@1.15.2/dist/bootstrap-table.min.css" rel="stylesheet">
            <script src="https://ajax.googleapis.com/ajax/libs/jquery/3.4.1/jquery.min.js"></script>
            <script src="https://unpkg.com/bootstrap-table@1.15.2/dist/bootstrap-table.min.js"></script>
            <script src="https://unpkg.com/bootstrap-table@1.15.2/dist/extensions/filter-control/bootstrap-table-filter-control.min.js"></script>
        </head>
        <body>
            {table}
            <script></script>
        </body>
    </html>
    """

    def __init__(self):
        return

    def write(self, dataframe: pd.DataFrame, filename: str):
        soup = BeautifulSoup(self.html_str.format(
            table=dataframe.to_html(
                classes=['table', 'table-striped', 'text-center'],
                table_id='aot_table', index=False)),
            'html.parser')
        soup.find("table", {"id": "aot_table"})['data-filter-control'] = "true"
        soup.find("table", {"id": "aot_table"})['data-filter-show-clear'] = "true"
        for th in soup.find_all('th'):
            if th.text in ['vsn', 'sensor', 'node_id', 'subsystem', 'parameter']:
                th['data-filter-control'] = "select"
            else:
                th['data-filter-control'] = "input"
            th["data-sortable"] = "true"

        soup.body.script.extend("""$(function() {
                  $('#aot_table').bootstrapTable()
           })""")

        with open(filename, 'w')as f:
            f.write(str(soup))
