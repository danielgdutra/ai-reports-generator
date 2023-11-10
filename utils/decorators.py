from datetime import datetime


def log(func):
    def wrapper(*args, **kwargs):
        result = {
            'service': func.__module__,
            'status': '❌',
            'row_count': 0,
            'error_message': ''
        }
        rows = 0
        print(f"{datetime.now()}: Started generating report {func.__module__} data")
        try:
            rows = func(*args, **kwargs)
        except Exception as e:
            result.update({'row_count': rows, 'error_message': str(e)})
            print(f"{datetime.now()}: Error generating report {func.__module__} data: {str(e)}")
        else:
            result.update({'status': '✅', 'row_count': rows})
            print(f"{datetime.now()}: Finished generating report {func.__module__} data")
        return result
    return wrapper
