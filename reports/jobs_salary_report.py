from utils import decorators, chatgpt_conn, sheets_conn
import pandas as pd

SPREADSHEET_ID = '17C8LzKiRSzJ6c_rZuG165M9v5ExGgi1f-LiVy_ZIKR4'
PIVOT_TABLE_REQUESTS = './assets/sheets_requests/jobs_salary_report.json'
DATASET_PATH = './assets/datasets/Salary.csv'
SHEET_FORMAT_MAPPINGS = {
    'F': 'CURRENCY'
}
PIVOT_TABLE_FORMAT_MAPPINGS = {
    'C': 'CURRENCY',
    'D': 'CURRENCY',
    'E': 'PERCENT',
    'K': 'PERCENT',
    'Q': 'PERCENT'
}
SHEET_AUTO_RESIZE_MAPPING = {
    'A': 'YES',
    'B': 'YES',
    'C': 'YES',
    'D': 'YES',
    'E': 'YES',
    'F': 'YES',
    'G': 'YES',
    'H': 'YES',
    'I': 'YES'
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
    'P': 'YES',
    'Q': 'YES'
}


def read_csv_to_list_of_lists(file_path):
    df = pd.read_csv(file_path)
    data_list = [df.columns.tolist()] + df.values.tolist()
    return data_list


@decorators.log
def generate_reports(start_date, end_date):
    full_data = read_csv_to_list_of_lists(DATASET_PATH)
    full_data_rows = len(full_data)
    full_data_columns = len(full_data[0])

    if full_data:
        sheets_conn.update_data(spreadsheet_id=SPREADSHEET_ID,
                                data_range='A1',
                                data=full_data,
                                target_sheet_name='Full Data'
                                )
        sheets_conn.add_alternating_colors(spreadsheet_id=SPREADSHEET_ID,
                                           target_sheet_name='Full Data',
                                           target_starting_row=0,
                                           target_final_row=full_data_rows,
                                           target_starting_column=0,
                                           target_final_column=full_data_columns
                                           )
        sheets_conn.format_range(spreadsheet_id=SPREADSHEET_ID,
                                 target_sheet_name='Full Data',
                                 format_mappings=SHEET_FORMAT_MAPPINGS
                                 )
        sheets_conn.add_basic_filter_to_all(spreadsheet_id=SPREADSHEET_ID,
                                            target_sheet_name=['Full Data']
                                            )
        sheets_conn.auto_resize_columns(spreadsheet_id=SPREADSHEET_ID,
                                        target_sheet_name=['Full Data'],
                                        auto_resize_mapping=SHEET_AUTO_RESIZE_MAPPING
                                        )
        sheets_conn.create_pivot_table(spreadsheet_id=SPREADSHEET_ID,
                                       source_sheet_name='Full Data',
                                       target_sheet_name='Pivot Tables',
                                       requests=PIVOT_TABLE_REQUESTS,
                                       source_starting_row=0,
                                       source_final_row=full_data_rows,
                                       source_starting_column=0,
                                       source_final_column=full_data_columns,
                                       target_starting_row=1,
                                       target_final_row=0,
                                       target_starting_column=0,
                                       target_final_column=0,
                                       pivot_table_agg_column=0,
                                       column_to_count='Age'
                                       )
        sheets_conn.update_data(spreadsheet_id=SPREADSHEET_ID,
                                data_range='C1',
                                data=[['Ages']],
                                target_sheet_name='Pivot Tables'
                                )
        sheets_conn.create_pivot_table(spreadsheet_id=SPREADSHEET_ID,
                                       source_sheet_name='Full Data',
                                       target_sheet_name='Pivot Tables',
                                       requests=PIVOT_TABLE_REQUESTS,
                                       source_starting_row=0,
                                       source_final_row=full_data_rows,
                                       source_starting_column=0,
                                       source_final_column=full_data_columns,
                                       target_starting_row=1,
                                       target_final_row=0,
                                       target_starting_column=6,
                                       target_final_column=0,
                                       pivot_table_agg_column=1,
                                       column_to_count='Gender'
                                       )
        sheets_conn.update_data(spreadsheet_id=SPREADSHEET_ID,
                                data_range='I1',
                                data=[['Genders']],
                                target_sheet_name='Pivot Tables'
                                )
        sheets_conn.create_pivot_table(spreadsheet_id=SPREADSHEET_ID,
                                       source_sheet_name='Full Data',
                                       target_sheet_name='Pivot Tables',
                                       requests=PIVOT_TABLE_REQUESTS,
                                       source_starting_row=0,
                                       source_final_row=full_data_rows,
                                       source_starting_column=0,
                                       source_final_column=full_data_columns,
                                       target_starting_row=7,
                                       target_final_row=0,
                                       target_starting_column=6,
                                       target_final_column=0,
                                       pivot_table_agg_column=2,
                                       column_to_count='Education Level'
                                       )
        sheets_conn.update_data(spreadsheet_id=SPREADSHEET_ID,
                                data_range='I7',
                                data=[['Education Levels']],
                                target_sheet_name='Pivot Tables'
                                )
        sheets_conn.create_pivot_table(spreadsheet_id=SPREADSHEET_ID,
                                       source_sheet_name='Full Data',
                                       target_sheet_name='Pivot Tables',
                                       requests=PIVOT_TABLE_REQUESTS,
                                       source_starting_row=0,
                                       source_final_row=full_data_rows,
                                       source_starting_column=0,
                                       source_final_column=full_data_columns,
                                       target_starting_row=1,
                                       target_final_row=0,
                                       target_starting_column=12,
                                       target_final_column=0,
                                       pivot_table_agg_column=3,
                                       column_to_count='Job Title'
                                       )
        sheets_conn.update_data(spreadsheet_id=SPREADSHEET_ID,
                                data_range='O1',
                                data=[['Job titles']],
                                target_sheet_name='Pivot Tables'
                                )
        sheets_conn.create_pivot_table(spreadsheet_id=SPREADSHEET_ID,
                                       source_sheet_name='Full Data',
                                       target_sheet_name='Pivot Tables',
                                       requests=PIVOT_TABLE_REQUESTS,
                                       source_starting_row=0,
                                       source_final_row=full_data_rows,
                                       source_starting_column=0,
                                       source_final_column=full_data_columns,
                                       target_starting_row=44,
                                       target_final_row=0,
                                       target_starting_column=6,
                                       target_final_column=0,
                                       pivot_table_agg_column=4,
                                       column_to_count='Years of Experience'
                                       )
        sheets_conn.update_data(spreadsheet_id=SPREADSHEET_ID,
                                data_range='I44',
                                data=[['Years of Experience']],
                                target_sheet_name='Pivot Tables'
                                )
        sheets_conn.create_pivot_table(spreadsheet_id=SPREADSHEET_ID,
                                       source_sheet_name='Full Data',
                                       target_sheet_name='Pivot Tables',
                                       requests=PIVOT_TABLE_REQUESTS,
                                       source_starting_row=0,
                                       source_final_row=full_data_rows,
                                       source_starting_column=0,
                                       source_final_column=full_data_columns,
                                       target_starting_row=15,
                                       target_final_row=0,
                                       target_starting_column=6,
                                       target_final_column=0,
                                       pivot_table_agg_column=6,
                                       column_to_count='Country'
                                       )
        sheets_conn.update_data(spreadsheet_id=SPREADSHEET_ID,
                                data_range='I15',
                                data=[['Countries']],
                                target_sheet_name='Pivot Tables'
                                )
        sheets_conn.create_pivot_table(spreadsheet_id=SPREADSHEET_ID,
                                       source_sheet_name='Full Data',
                                       target_sheet_name='Pivot Tables',
                                       requests=PIVOT_TABLE_REQUESTS,
                                       source_starting_row=0,
                                       source_final_row=full_data_rows,
                                       source_starting_column=0,
                                       source_final_column=full_data_columns,
                                       target_starting_row=24,
                                       target_final_row=0,
                                       target_starting_column=6,
                                       target_final_column=0,
                                       pivot_table_agg_column=7,
                                       column_to_count='Ethnicity'
                                       )
        sheets_conn.update_data(spreadsheet_id=SPREADSHEET_ID,
                                data_range='I24',
                                data=[['Ethnicities']],
                                target_sheet_name='Pivot Tables'
                                )
        sheets_conn.create_pivot_table(spreadsheet_id=SPREADSHEET_ID,
                                       source_sheet_name='Full Data',
                                       target_sheet_name='Pivot Tables',
                                       requests=PIVOT_TABLE_REQUESTS,
                                       source_starting_row=0,
                                       source_final_row=full_data_rows,
                                       source_starting_column=0,
                                       source_final_column=full_data_columns,
                                       target_starting_row=38,
                                       target_final_row=0,
                                       target_starting_column=6,
                                       target_final_column=0,
                                       pivot_table_agg_column=8,
                                       column_to_count='Senior'
                                       )
        sheets_conn.update_data(spreadsheet_id=SPREADSHEET_ID,
                                data_range='I38',
                                data=[['Seniority']],
                                target_sheet_name='Pivot Tables'
                                )
        sheets_conn.format_range(spreadsheet_id=SPREADSHEET_ID,
                                 target_sheet_name='Pivot Tables',
                                 format_mappings=PIVOT_TABLE_FORMAT_MAPPINGS
                                 )
        sheets_conn.auto_resize_columns(spreadsheet_id=SPREADSHEET_ID,
                                        target_sheet_name='Pivot Tables',
                                        auto_resize_mapping=PIVOT_TABLE_AUTO_RESIZE_MAPPING
                                        )
        sheets_conn.create_chart(spreadsheet_id=SPREADSHEET_ID,
                                 source_sheet_name='Full Data',
                                 target_sheet_name='Charts',
                                 x_axis_column_name='Age',
                                 y_axis_column_name='Salary',
                                 end_column=full_data_columns,
                                 chart_request='./assets/sheets_requests/jobs_salary_report/age_chart.json'
                                 )
        sheets_conn.create_chart(spreadsheet_id=SPREADSHEET_ID,
                                 source_sheet_name='Full Data',
                                 target_sheet_name='Charts',
                                 x_axis_column_name='Gender',
                                 y_axis_column_name='Salary',
                                 end_column=full_data_columns,
                                 chart_request='./assets/sheets_requests/jobs_salary_report/gender_chart.json'
                                 )
        sheets_conn.create_chart(spreadsheet_id=SPREADSHEET_ID,
                                 source_sheet_name='Full Data',
                                 target_sheet_name='Charts',
                                 x_axis_column_name='Education Level',
                                 y_axis_column_name='Salary',
                                 end_column=full_data_columns,
                                 chart_request='./assets/sheets_requests/jobs_salary_report/education_level_chart.json'
                                 )
        sheets_conn.create_chart(spreadsheet_id=SPREADSHEET_ID,
                                 source_sheet_name='Full Data',
                                 target_sheet_name='Charts',
                                 x_axis_column_name='Years of Experience',
                                 y_axis_column_name='Salary',
                                 end_column=full_data_columns,
                                 chart_request='./assets/sheets_requests/jobs_salary_report/years_of_exp_chart.json'
                                 )
        sheets_conn.create_chart(spreadsheet_id=SPREADSHEET_ID,
                                 source_sheet_name='Full Data',
                                 target_sheet_name='Charts',
                                 x_axis_column_name='Job Title',
                                 y_axis_column_name='Salary',
                                 end_column=full_data_columns,
                                 chart_request='./assets/sheets_requests/jobs_salary_report/job_title_chart.json'
                                 )
        sheets_conn.create_chart(spreadsheet_id=SPREADSHEET_ID,
                                 source_sheet_name='Full Data',
                                 target_sheet_name='Charts',
                                 x_axis_column_name='Country',
                                 y_axis_column_name='Salary',
                                 end_column=full_data_columns,
                                 chart_request='./assets/sheets_requests/jobs_salary_report/country_chart.json'
                                 )
        sheets_conn.create_chart(spreadsheet_id=SPREADSHEET_ID,
                                 source_sheet_name='Full Data',
                                 target_sheet_name='Charts',
                                 x_axis_column_name='Ethnicity',
                                 y_axis_column_name='Salary',
                                 end_column=full_data_columns,
                                 chart_request='./assets/sheets_requests/jobs_salary_report/ethnicity_chart.json'
                                 )
        sheets_conn.create_chart(spreadsheet_id=SPREADSHEET_ID,
                                 source_sheet_name='Full Data',
                                 target_sheet_name='Charts',
                                 x_axis_column_name='Senior',
                                 y_axis_column_name='Salary',
                                 end_column=full_data_columns,
                                 chart_request='./assets/sheets_requests/jobs_salary_report/senior_chart.json'
                                 )
        # print(chatgpt_conn.generate_insights(full_data).to_txt)
    return len(full_data) if full_data else 0
