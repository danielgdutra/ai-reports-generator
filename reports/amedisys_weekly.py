from utils import decorators
from bigquery_conn import BigQueryConn
import sheets_conn, chatgpt_conn
from decimal import Decimal


DATASET_ID = 'tfy_warehouse'
TABLE_ID = 'v_channel_flow'
QUERY_PATH = './assets/queries/amedisys_reference_week.sql'
SPREADSHEET_ID = '1qf7BEKBznqWPEy7Aw3PZQAkh5D6iD7B3adv0Q-0Y2B8'
PIVOT_TABLE_REQUESTS = './assets/sheets_requests/pivot_table_amedisys_weekly.json'
SHEET_FORMAT_MAPPINGS = {
    'L': 'PERCENT',
    'M': 'CURRENCY',
    'N': 'PERCENT',
    'O': 'PERCENT',
    'P': 'PERCENT',
    'Q': 'CURRENCY',
    'R': 'CURRENCY',
    'S': 'PERCENT',
    'T': 'PERCENT',
    'U': 'PERCENT'
}
PIVOT_TABLE_FORMAT_MAPPINGS = {
    'G': 'PERCENT',
    'H': 'CURRENCY',
    'I': 'PERCENT',
    'J': 'PERCENT',
    'K': 'PERCENT',
    'L': 'CURRENCY',
    'M': 'CURRENCY',
    'N': 'NUMBER',
    'O': 'NUMBER',
    'P': 'NUMBER'
}
PIVOT_TABLE_HEADER_MAPPING = {
    'A': 'Week',
    'B': 'Total Jobs',
    'C': 'Impressions',
    'D': 'Clicks',
    'E': 'Apply Starts',
    'F': 'Applies',
    'G': 'Conversion Rate',
    'H': 'Spent',
    'I': 'CTR',
    'J': 'Clicks to Apply Starts',
    'K': 'Clicks to Applies',
    'L': 'CPC',
    'M': 'CPA',
    'N': 'Clicks / Total Jobs',
    'O': 'Apply Start / Total Jobs',
    'P': 'Applies / Total Jobs'
}
SHEET_AUTO_RESIZE_MAPPING = {
    'A': 'YES',
    'B': 'YES',
    'C': 'NO',
    'D': 'YES',
    'E': 'YES',
    'F': 'YES',
    'G': 'YES',
    'H': 'YES',
    'I': 'YES',
    'J': 'YES',
    'K': 'YES',
    'L': 'YES',
    'M': 'YES',
    'N': 'YES',
    'O': 'YES',
    'P': 'YES',
    'Q': 'YES',
    'R': 'YES',
    'S': 'YES',
    'T': 'YES',
    'U': 'YES'
}
PIVOT_TABLE_AUTO_RESIZE_MAPPING = {
    'A': 'YES',
    'B': 'YES',
    'C': 'YES',
    'D': 'YES',
    'E': 'YES',
    'F': 'YES',
    'G': 'YES',
    'H': 'YES',
    'I': 'YES',
    'J': 'YES',
    'K': 'YES',
    'L': 'YES',
    'M': 'YES',
    'N': 'YES',
    'O': 'YES',
    'P': 'YES'
}


def read_file(text):
    with open(text) as f:
        query_text = f.read()
    return query_text


def get_report_data(start_date, end_date):
    sql = read_file(QUERY_PATH).format(
        start_date=start_date.strftime('%Y-%m-%d'),
        end_date=end_date.strftime('%Y-%m-%d')
    )

    conn = BigQueryConn(DATASET_ID, TABLE_ID)
    run_query = conn.query_data(query=sql)
    query_results = run_query.result()

    result_dicts = [dict(row.items()) for row in query_results]

    if not result_dicts:
        raise Exception("The query found no results.")

    headers = list(result_dicts[0].keys())
    result_list_of_lists = [headers]

    for row_dict in result_dicts:
        row_values = [float(value) if isinstance(value, Decimal) else value for value in row_dict.values()]
        result_list_of_lists.append(row_values)

    return result_list_of_lists


@decorators.log
def generate_reports(start_date, end_date):
    all_jobs_data = get_report_data(start_date, end_date)
    all_jobs_data_rows = len(all_jobs_data)
    all_jobs_data_columns = len(all_jobs_data[0])

    if all_jobs_data:
        """sheets_conn.update_data(spreadsheet_id=SPREADSHEET_ID,
                                data_range='A1',
                                data=all_jobs_data,
                                target_sheet_name='Critical Care'
                                )
        sheets_conn.add_alternating_colors(spreadsheet_id=SPREADSHEET_ID,
                                           target_sheet_name='Critical Care',
                                           target_starting_row=0,
                                           target_final_row=all_jobs_data_rows,
                                           target_starting_column=0,
                                           target_final_column=all_jobs_data_columns
                                           )
        sheets_conn.format_range(spreadsheet_id=SPREADSHEET_ID,
                                 target_sheet_name='Critical Care',
                                 format_mappings=SHEET_FORMAT_MAPPINGS
                                 )
        sheets_conn.add_basic_filter_to_all(spreadsheet_id=SPREADSHEET_ID,
                                            target_sheet_name='Critical Care')
        sheets_conn.auto_resize_columns(spreadsheet_id=SPREADSHEET_ID,
                                        target_sheet_name='Critical Care',
                                        auto_resize_mapping=SHEET_AUTO_RESIZE_MAPPING)
        sheets_conn.create_pivot_table(spreadsheet_id=SPREADSHEET_ID,
                                       source_sheet_name='Critical Care',
                                       target_sheet_name='Report',
                                       requests=PIVOT_TABLE_REQUESTS,
                                       source_starting_row=0,
                                       source_final_row=all_jobs_data_rows,
                                       source_starting_column=0,
                                       source_final_column=all_jobs_data_columns,
                                       target_starting_row=0,
                                       target_final_row=0,
                                       target_starting_column=0,
                                       target_final_column=0
                                       )
        sheets_conn.format_range(spreadsheet_id=SPREADSHEET_ID,
                                 target_sheet_name='Report',
                                 format_mappings=PIVOT_TABLE_FORMAT_MAPPINGS
                                 )
        sheets_conn.auto_resize_columns(spreadsheet_id=SPREADSHEET_ID,
                                        target_sheet_name='Report',
                                        auto_resize_mapping=PIVOT_TABLE_AUTO_RESIZE_MAPPING
                                        )"""
        print(chatgpt_conn.generate_insights(all_jobs_data).to_txt)
    return len(all_jobs_data) if all_jobs_data else 0
