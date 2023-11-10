from utils import decorators
from bigquery_conn import BigQueryConn
import sheets_conn, chatgpt_conn
from decimal import Decimal


DATASET_ID = 'tfy_warehouse'
TABLE_ID = 'v_channel_flow'
QUERY_PATH = './assets/queries/amedisys_reference_month.sql'
SPREADSHEET_ID = '1axq-uIZZIzLOUBmqr8Ou_njEaGsU5TFA8OvIeMhztaw'
PIVOT_TABLE_REQUESTS = './assets/sheets_requests/pivot_table_amedisys_monthly.json'
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


def read_query_text(query):
    with open(query) as f:
        query_text = f.read()
    return query_text


def get_report_data(start_date, end_date):
    sql = read_query_text(QUERY_PATH).format(
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
    all_jobs_header = [['All Jobs']]

    hospice_jobs_substrings = ["RN", "Registered Nurse", "Aide"]
    hospice_jobs_filtered_data = [all_jobs_data[0]]
    hospice_jobs_filtered_data += [row for row in all_jobs_data if any(sub in row[2] for sub in hospice_jobs_substrings)]
    hospice_jobs_data_rows = len(hospice_jobs_filtered_data)
    hospice_jobs_data_columns = len(hospice_jobs_filtered_data[0])
    hospice_jobs_header = [['Hospice']]

    nurse_jobs_substrings = ["RN", "Registered Nurse"]
    nurse_jobs_filtered_data = [all_jobs_data[0]]
    nurse_jobs_filtered_data += [row for row in all_jobs_data if any(sub in row[2] for sub in nurse_jobs_substrings)]
    nurse_jobs_data_rows = len(nurse_jobs_filtered_data)
    nurse_jobs_data_columns = len(nurse_jobs_filtered_data[0])
    nurse_jobs_header = [['Nurse']]

    nursing_jobs_substrings = ["RN", "LPN", "Nurse"]
    nursing_jobs_exclusions = ["CNA", "Aide"]
    nursing_jobs_filtered_data = [all_jobs_data[0]]
    nursing_jobs_filtered_data += [row for row in all_jobs_data[2:]
                                  if any(inc in row[2] for inc in nursing_jobs_substrings) and
                                  not any(exc in row[2] for exc in nursing_jobs_exclusions)]
    nursing_jobs_data_rows = len(nursing_jobs_filtered_data)
    nursing_jobs_data_columns = len(nursing_jobs_filtered_data[0])
    nursing_jobs_header = [['Nurse All']]

    if all_jobs_data:
        """sheets_conn.update_data(spreadsheet_id=SPREADSHEET_ID,
                                data_range='A1',
                                data=all_jobs_data,
                                target_sheet_name='All Jobs Data'
                                )
        sheets_conn.add_alternating_colors(spreadsheet_id=SPREADSHEET_ID,
                                           target_sheet_name='All Jobs Data',
                                           target_starting_row=0,
                                           target_final_row=all_jobs_data_rows,
                                           target_starting_column=0,
                                           target_final_column=all_jobs_data_columns
                                           )
        sheets_conn.format_range(spreadsheet_id=SPREADSHEET_ID,
                                 target_sheet_name='All Jobs Data',
                                 format_mappings=SHEET_FORMAT_MAPPINGS
                                 )
        sheets_conn.update_data(spreadsheet_id=SPREADSHEET_ID,
                                data_range='A1',
                                data=hospice_jobs_filtered_data,
                                target_sheet_name="Hospice Jobs Data"
                                )
        sheets_conn.add_alternating_colors(spreadsheet_id=SPREADSHEET_ID,
                                           target_sheet_name='Hospice Jobs Data',
                                           target_starting_row=0,
                                           target_final_row=hospice_jobs_data_rows,
                                           target_starting_column=0,
                                           target_final_column=hospice_jobs_data_columns
                                           )
        sheets_conn.format_range(spreadsheet_id=SPREADSHEET_ID,
                                 target_sheet_name='Hospice Jobs Data',
                                 format_mappings=SHEET_FORMAT_MAPPINGS
                                 )
        sheets_conn.update_data(spreadsheet_id=SPREADSHEET_ID,
                                data_range='A1',
                                data=nurse_jobs_filtered_data,
                                target_sheet_name='Nurse Jobs Data'
                                )
        sheets_conn.add_alternating_colors(spreadsheet_id=SPREADSHEET_ID,
                                           target_sheet_name='Nurse Jobs Data',
                                           target_starting_row=0,
                                           target_final_row=nurse_jobs_data_rows,
                                           target_starting_column=0,
                                           target_final_column=nurse_jobs_data_columns
                                           )
        sheets_conn.format_range(spreadsheet_id=SPREADSHEET_ID,
                                 target_sheet_name='Nurse Jobs Data',
                                 format_mappings=SHEET_FORMAT_MAPPINGS
                                 )
        sheets_conn.update_data(spreadsheet_id=SPREADSHEET_ID,
                                data_range='A1',
                                data=nursing_jobs_filtered_data,
                                target_sheet_name='Nursing Jobs Data'
                                )
        sheets_conn.add_alternating_colors(spreadsheet_id=SPREADSHEET_ID,
                                           target_sheet_name='Nursing Jobs Data',
                                           target_starting_row=0,
                                           target_final_row=nursing_jobs_data_rows,
                                           target_starting_column=0,
                                           target_final_column=nursing_jobs_data_columns
                                           )
        sheets_conn.format_range(spreadsheet_id=SPREADSHEET_ID,
                                 target_sheet_name='Nursing Jobs Data',
                                 format_mappings=SHEET_FORMAT_MAPPINGS
                                 )
        sheets_conn.add_basic_filter_to_all(spreadsheet_id=SPREADSHEET_ID,
                                            target_sheet_name=['All Jobs Data', 'Hospice Jobs Data',
                                                               'Nurse Jobs Data', 'Nursing Jobs Data']
                                            )
        sheets_conn.auto_resize_columns(spreadsheet_id=SPREADSHEET_ID,
                                        target_sheet_name=['All Jobs Data', 'Hospice Jobs Data',
                                                           'Nurse Jobs Data', 'Nursing Jobs Data'],
                                        auto_resize_mapping=SHEET_AUTO_RESIZE_MAPPING
                                        )
        sheets_conn.create_pivot_table(spreadsheet_id=SPREADSHEET_ID,
                                       source_sheet_name='All Jobs Data',
                                       target_sheet_name='Report 1',
                                       requests=PIVOT_TABLE_REQUESTS,
                                       source_starting_row=0,
                                       source_final_row=all_jobs_data_rows,
                                       source_starting_column=0,
                                       source_final_column=all_jobs_data_columns,
                                       target_starting_row=22,
                                       target_final_row=0,
                                       target_starting_column=0,
                                       target_final_column=0
                                       )
        sheets_conn.update_data(spreadsheet_id=SPREADSHEET_ID,
                                data_range='H22',
                                data=all_jobs_header,
                                target_sheet_name='Report 1'
                                )
        sheets_conn.create_pivot_table(spreadsheet_id=SPREADSHEET_ID,
                                       source_sheet_name='Hospice Jobs Data',
                                       target_sheet_name='Report 1',
                                       requests=PIVOT_TABLE_REQUESTS,
                                       source_starting_row=0,
                                       source_final_row=hospice_jobs_data_rows,
                                       source_starting_column=0,
                                       source_final_column=hospice_jobs_data_columns,
                                       target_starting_row=1,
                                       target_final_row=0,
                                       target_starting_column=0,
                                       target_final_column=0
                                       )
        sheets_conn.update_data(spreadsheet_id=SPREADSHEET_ID,
                                data_range='H1',
                                data=hospice_jobs_header,
                                target_sheet_name='Report 1'
                                )
        sheets_conn.create_pivot_table(spreadsheet_id=SPREADSHEET_ID,
                                       source_sheet_name='Nurse Jobs Data',
                                       target_sheet_name='Report 1',
                                       requests=PIVOT_TABLE_REQUESTS,
                                       source_starting_row=0,
                                       source_final_row=nurse_jobs_data_rows,
                                       source_starting_column=0,
                                       source_final_column=nurse_jobs_data_columns,
                                       target_starting_row=8,
                                       target_final_row=0,
                                       target_starting_column=0,
                                       target_final_column=0
                                       )
        sheets_conn.update_data(spreadsheet_id=SPREADSHEET_ID,
                                data_range='H8',
                                data=nurse_jobs_header,
                                target_sheet_name='Report 1'
                                )
        sheets_conn.create_pivot_table(spreadsheet_id=SPREADSHEET_ID,
                                       source_sheet_name='Nursing Jobs Data',
                                       target_sheet_name='Report 1',
                                       requests=PIVOT_TABLE_REQUESTS,
                                       source_starting_row=0,
                                       source_final_row=nursing_jobs_data_rows,
                                       source_starting_column=0,
                                       source_final_column=nursing_jobs_data_columns,
                                       target_starting_row=15,
                                       target_final_row=0,
                                       target_starting_column=0,
                                       target_final_column=0
                                       )
        sheets_conn.update_data(spreadsheet_id=SPREADSHEET_ID,
                                data_range='H15',
                                data=nursing_jobs_header,
                                target_sheet_name='Report 1'
                                )
        sheets_conn.format_range(spreadsheet_id=SPREADSHEET_ID,
                                 target_sheet_name='Report 1',
                                 format_mappings=PIVOT_TABLE_FORMAT_MAPPINGS
                                 )
        sheets_conn.auto_resize_columns(spreadsheet_id=SPREADSHEET_ID,
                                        target_sheet_name='Report 1',
                                        auto_resize_mapping=PIVOT_TABLE_AUTO_RESIZE_MAPPING
                                        )"""
        print(chatgpt_conn.generate_insights(all_jobs_data).to_txt)
    return len(all_jobs_data) if all_jobs_data else 0
