import argparse
from datetime import datetime, timedelta, date
from utils import mail_service
from reports import amedisys_weekly, amedisys_monthly


def run_sync(job, start_date, end_date):
    try:
        if start_date and end_date:
            return job.generate_reports(start_date, end_date)
    except Exception as e:
        return {'status': '❌', 'error': str(e)}


def run_weekly_reports_generator(start_date, end_date):
    reports = [amedisys_weekly]
    # Run jobs and attach the rows and errors to the email
    sync_data = [run_sync(job, start_date, end_date) for job in reports]
    html = mail_service.generate_html(sync_data)
    # Send Mail
    mail_service.send_mail(f"Report Generator from {start_date.strftime('%d/%m/%Y')} to {end_date.strftime('%d/%m/%Y')}", html)


def run_monthly_reports_generator(start_date, end_date):
    reports = [amedisys_monthly]
    # Run jobs and attach the rows and errors to the email
    sync_data = [run_sync(job, start_date, end_date) for job in reports]
    html = mail_service.generate_html([data for data in sync_data if data['status'] == '❌' or data['row_count'] == 0])
    # Send Mail
    mail_service.send_mail(f"Report Generator from {start_date.strftime('%d/%m/%Y')} to {end_date.strftime('%d/%m/%Y')}", html)


def main(start_date: date, end_date: date):
    today = date.today()

    #if today.weekday() == 0:
    #    run_weekly_reports_generator(start_date,end_date)
    # Uncomment next if to test new reports
    if today.weekday() in (0,1,2,3,4,5,6):
        run_weekly_reports_generator(start_date,end_date)
        run_monthly_reports_generator(start_date, end_date)

    #if today.day == 1:
    #    run_monthly_reports_generator(start_date,end_date)


if __name__ == '__main__':
    # Create parser
    parser = argparse.ArgumentParser(description="Reports Generator.")
    parser.add_argument('-s', '--start_date', action='store', dest='start_date', required=False,
                        help='The start date of the report, default is 7 days ago')
    parser.add_argument('-e', '--end_date', action='store', dest='end_date', required=False,
                        help='The end date of the report, default is today')
    arguments = parser.parse_args()
    # Parse args
    start_date = datetime.strptime(arguments.start_date, '%Y-%m-%d').date() if arguments.start_date else datetime.today().date() - timedelta(days=7)
    end_date = datetime.strptime(arguments.end_date, '%Y-%m-%d').date() if arguments.end_date else datetime.today().date()

    # run main func
    main(start_date, end_date)
