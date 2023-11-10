import os, json
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from google.auth.exceptions import DefaultCredentialsError
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from string import Template


SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
CREDENTIALS_FILE = './credentials.json'


def convert_int_keys(data, int_keys):
    if isinstance(data, dict):
        for key, value in data.items():
            if key in int_keys:
                if isinstance(value, str) and value.isdigit():
                    data[key] = int(value)
            elif isinstance(value, (dict, list)):
                convert_int_keys(value, int_keys)
    elif isinstance(data, list):
        for item in data:
            convert_int_keys(item, int_keys)


def replace_json_placeholders(json_data, **kwargs):
    with open(json_data, 'r') as file:
        json_str = file.read()

    template = Template(json_str)
    rendered_str = template.substitute(kwargs)

    formatted_json = json.loads(rendered_str)

    int_keys = [
        'startRowIndex', 'startColumnIndex', 'endRowIndex', 'endColumnIndex',
        'rowIndex', 'columnIndex', 'sheetId', 'sourceColumnOffset'
    ]

    convert_int_keys(formatted_json, int_keys)
    return formatted_json


def connect_to_sheets():
    creds = None
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)

        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    try:
        service = build('sheets', 'v4', credentials=creds)
        sheet = service.spreadsheets()
    except DefaultCredentialsError as e:
        print(f"Could not load Google credentials: {e}")
        sheet = None
    return sheet


def create_spreadsheet(spreadsheet_title):
    sheet_service = connect_to_sheets()

    if sheet_service:
        try:
            spreadsheet_body = {
                'properties': {
                    'title': spreadsheet_title
                }
            }
            request = sheet_service.create(body=spreadsheet_body).execute()
            new_spreadsheet_id = request.get('spreadsheetId')
            print(f"Spreadsheet created with ID: {new_spreadsheet_id}")
            return new_spreadsheet_id
        except Exception as e:
            print(f"An error occurred: {e}")
            return None
    else:
        print("Failed to connect to the Google Sheets service.")
        return None


def add_new_sheet_to_spreadsheet(spreadsheet_id, sheet_name):
    sheet_service = connect_to_sheets()
    sheet_metadata = sheet_service.get(spreadsheetId=spreadsheet_id).execute()
    sheets = sheet_metadata.get('sheets', '')
    sheet_exists = any(s['properties']['title'] == sheet_name for s in sheets)

    if not sheet_exists:
        print(f"No sheet with name {sheet_name} found. Creating new sheet.")
        add_sheet_request = {
            "addSheet": {
                "properties": {
                    "title": sheet_name
                }
            }
        }
        batch_update_response = sheet_service.batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": [add_sheet_request]}
        ).execute()
        new_sheet_id = batch_update_response.get('replies')[0].get('addSheet').get('properties').get('sheetId')
        print(f"New sheet '{sheet_name}' created with ID: {new_sheet_id}")
        return new_sheet_id
    else:
        for s in sheets:
            if s['properties']['title'] == sheet_name:
                return s['properties']['sheetId']


def update_data(spreadsheet_id, data_range, data, target_sheet_name):
    sheet_service = connect_to_sheets()

    if sheet_service:
        try:
            add_new_sheet_to_spreadsheet(spreadsheet_id, target_sheet_name)

            data_range = f"{target_sheet_name}!{data_range}"

            body = {'values': data}
            result = sheet_service.values().update(
                spreadsheetId=spreadsheet_id, range=data_range,
                valueInputOption='RAW', body=body
            ).execute()

            print(f"{result.get('updatedCells')} cells updated.")
            return result
        except Exception as e:
            print(f"An error occurred: {e}")
            return None
    else:
        print("Failed to connect to the Google Sheets service.")
        return None


def append_data(spreadsheet_id, data_range, data):
    try:
        # Connect to the Google Sheets API
        sheets_service = connect_to_sheets()

        # Prepare the request body
        body = {
            'values': data
        }

        # Use the append API
        result = sheets_service.values().append(
            spreadsheetId=spreadsheet_id,
            range=data_range,
            valueInputOption='USER_ENTERED',
            insertDataOption='INSERT_ROWS',
            body=body
        ).execute()

        print(f"Appended data to {data_range} in spreadsheet {spreadsheet_id}.")
        return result
    except Exception as e:
        print(f"An error occurred while appending data to sheet: {e}")
        raise


def create_pivot_table(spreadsheet_id, source_sheet_name, target_sheet_name, requests,
                       source_starting_row, source_final_row, source_starting_column, source_final_column,
                       target_starting_row, target_final_row, target_starting_column, target_final_column,
                       pivot_table_header_mapping=None):
    sheet_service = connect_to_sheets()

    if sheet_service:
        try:
            source_sheet_id = add_new_sheet_to_spreadsheet(spreadsheet_id, source_sheet_name)
            target_sheet_id = add_new_sheet_to_spreadsheet(spreadsheet_id, target_sheet_name)

            pivot_table_requests = replace_json_placeholders(
                json_data=requests,
                target_sheet_id=target_sheet_id,
                source_sheet_id=source_sheet_id,
                source_starting_row=source_starting_row,
                source_final_row=source_final_row,
                source_starting_column=source_starting_column,
                source_final_column=source_final_column,
                target_starting_row=target_starting_row,
                target_final_row=target_final_row,
                target_starting_column=target_starting_column,
                target_final_column=target_final_column
            )

            body = {"requests": pivot_table_requests}
            # Use the 'sheet_service' object to call the batchUpdate method with the correct sheetId
            response = sheet_service.batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()

            # After creating the pivot table, update the headers and set their color
            if pivot_table_header_mapping:
                update_headers_and_set_color(spreadsheet_id, target_sheet_name, pivot_table_header_mapping,
                                             target_starting_row)

            return response
        except Exception as e:
            print(f"An error occurred: {e}")
            return None
    else:
        print("Failed to connect to the Google Sheets service.")
        return None


def add_alternating_colors(spreadsheet_id, target_sheet_name, target_starting_row, target_final_row,
                           target_starting_column, target_final_column):
    service = connect_to_sheets()

    sheet_metadata = service.get(spreadsheetId=spreadsheet_id).execute()
    sheets = sheet_metadata.get('sheets', [])
    sheet_id = next((s['properties']['sheetId'] for s in sheets if s['properties']['title'] == target_sheet_name), None)
    sheet = next((s for s in sheet_metadata.get('sheets', []) if s['properties']['sheetId'] == sheet_id), None)

    if sheet and 'bandedRanges' in sheet:
        print("Alternating colors already exist.")
        return

    alternate_colors_requests = replace_json_placeholders(
        json_data='./assets/sheets_requests/alternating_colors.json',
        target_sheet_id=sheet_id,
        target_starting_row=target_starting_row,
        target_final_row=target_final_row,
        target_starting_column=target_starting_column,
        target_final_column=target_final_column
        )

    body = {"requests": alternate_colors_requests}

    response = service.batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()
    return response


def format_range(spreadsheet_id, target_sheet_name, format_mappings):
    service = connect_to_sheets()
    sheet_metadata = service.get(spreadsheetId=spreadsheet_id).execute()
    sheets = sheet_metadata.get('sheets', [])
    sheet_id = next((s['properties']['sheetId'] for s in sheets if s['properties']['title'] == target_sheet_name), None)
    requests = []

    number_formats = {
        'CURRENCY': {"type": "CURRENCY", "pattern": "[$$-409]#,##0.00"},
        'PERCENT': {"type": "PERCENT", "pattern": "0.00%"},
        'NUMBER': {"type": "NUMBER", "pattern": "0.00"}
    }

    column_base = ord('A') - 1

    for column_letter, format_type in format_mappings.items():
        column_index = ord(column_letter.upper()) - column_base
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startColumnIndex": column_index - 1,
                    "endColumnIndex": column_index
                },
                "cell": {
                    "userEnteredFormat": {
                        "numberFormat": number_formats[format_type]
                    }
                },
                "fields": "userEnteredFormat.numberFormat"
            }
        })

    body = {"requests": requests}
    response = service.batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()
    return response


def update_headers_and_set_color(spreadsheet_id, target_sheet_name, pivot_table_header_mapping, target_starting_row):
    header_color = {"red": 0.6, "green": 0.6, "blue": 0.6}
    headers = [pivot_table_header_mapping[col] for col in sorted(pivot_table_header_mapping)]
    data = [headers]

    if target_starting_row <= 1:
        data_range = f"A1:{chr(ord('A') + len(headers) - 1)}1"
    else:
        data_range = f"A{target_starting_row}:{chr(ord('A') + len(headers) - 1)}{target_starting_row}"

    update_data(spreadsheet_id, data_range, data, target_sheet_name)

    service = connect_to_sheets()
    sheet_metadata = service.get(spreadsheetId=spreadsheet_id).execute()
    sheets = sheet_metadata.get('sheets', [])
    sheet_id = next(
        (sheet['properties']['sheetId'] for sheet in sheets if sheet['properties']['title'] == target_sheet_name), None)

    if sheet_id is None:
        raise ValueError(f"Sheet named '{target_sheet_name}' not found.")

    requests = [{
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": target_starting_row - 1,
                "endRowIndex": target_starting_row,
                "startColumnIndex": 0,
                "endColumnIndex": len(headers)
            },
            "cell": {
                "userEnteredFormat": {
                    "backgroundColor": header_color
                }
            },
            "fields": "userEnteredFormat.backgroundColor"
        }
    }]

    body = {"requests": requests}
    response = service.batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()

    return response


def auto_resize_columns(spreadsheet_id, target_sheet_name, auto_resize_mapping):
    service = connect_to_sheets()

    if isinstance(target_sheet_name, str):
        target_sheet_name = [target_sheet_name]

    responses = []

    for target_sheet_name in target_sheet_name:
        sheet_metadata = service.get(spreadsheetId=spreadsheet_id).execute()
        sheets = sheet_metadata.get('sheets', [])
        sheet_id = next((s['properties']['sheetId'] for s in sheets if s['properties']['title'] == target_sheet_name), None)

        if sheet_id is None:
            raise ValueError(f"Sheet named '{target_sheet_name}' not found.")

        requests = []

        for column, should_auto_resize in auto_resize_mapping.items():
            if should_auto_resize.upper() == 'YES':
                column_index = ord(column) - ord('A')
                requests.append({
                    "autoResizeDimensions": {
                        "dimensions": {
                            "sheetId": sheet_id,
                            "dimension": "COLUMNS",
                            "startIndex": column_index,
                            "endIndex": column_index + 1
                        }
                    }
                })

        if requests:
            body = {'requests': requests}
            response = service.batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()
            responses.append(response)

    return responses if responses else None


def add_basic_filter_to_all(spreadsheet_id, target_sheet_name):
    service = connect_to_sheets()
    sheet_metadata = service.get(spreadsheetId=spreadsheet_id).execute()
    sheets = sheet_metadata.get('sheets', '')

    if isinstance(target_sheet_name, str):
        target_sheet_name = [target_sheet_name]

    requests = []

    # Add a filter for each sheet name provided
    for sheet_name in target_sheet_name:
        sheet_id = next((sheet['properties']['sheetId'] for sheet in sheets if sheet['properties']['title'] == sheet_name), None)

        if sheet_id is not None:
            requests.append({
                "setBasicFilter": {
                    "filter": {
                        "range": {
                            "sheetId": sheet_id
                        }
                    }
                }
            })
        else:
            print(f"Sheet named '{sheet_name}' not found.")

    if not requests:
        print("No valid sheet names provided.")
        return None

    body = {'requests': requests}
    response = service.batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()
    return response
