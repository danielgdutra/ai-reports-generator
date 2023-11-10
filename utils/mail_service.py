import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import os
from dotenv import load_dotenv

load_dotenv()
def generate_html(data):
    content = ""
    if len(data) > 0:
        for d in data:
            content += f"""
            <tr>
              <td>{d['service']}</td>
              <td>{d['status']}</td>
              <td>{d['row_count']}</td>
              <td>{d['error_message']}</td>
            </tr>"""
    else:
        content += """
        <tr>
          <td></td>
          <td></td>
          <td></td>
          <td></td>
        </tr>"""
    return """
    <!DOCTYPE html>
    <html>
      <head>
        <meta charset="utf-8" />
        <style type="text/css">
          table {
            background: white;
            border-radius:3px;
            border-collapse: collapse;
            height: auto;
            max-width: 900px;
            padding:5px;
            width: 100%;
            animation: float 5s infinite;
          }
          th {
            color:#D5DDE5;;
            background:#1b1e24;
            border-bottom: 4px solid #9ea7af;
            font-size:14px;
            font-weight: 300;
            padding:10px;
            text-align:center;
            vertical-align:middle;
          }
          tr {
            border-top: 1px solid #C1C3D1;
            border-bottom: 1px solid #C1C3D1;
            border-left: 1px solid #C1C3D1;
            color:#666B85;
            font-size:16px;
            font-weight:normal;
          }
          tr:hover td {
            background:#4E5066;
            color:#FFFFFF;
            border-top: 1px solid #22262e;
          }
          td {
            background:#FFFFFF;
            padding:10px;
            text-align:left;
            vertical-align:middle;
            font-weight:300;
            font-size:13px;
            border-right: 1px solid #C1C3D1;
          }
        </style>
      </head>
      <body>
        Status of sync:<br> <br>
        <table>
          <thead>
            <tr style="border: 1px solid #1b1e24;">
              <th>Service</th>
              <th>Status</th>
              <th>Row Count</th>
              <th>Error Message</th>
            </tr>
          </thead>
          <tbody>
        """ + content + """
         </tbody>
        </table>
      </body>
    </html>
    """



def send_mail(subject, body):
    subject = subject
    body = body
    sender_email = "daniel.dutra@talentify.io"
    receiver_email = "daniel.dutra@talentify.io"
    password = os.getenv("EMAIL_PASSWORD")

    # Create a multipart message and set headers
    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = receiver_email
    message["Subject"] = subject

    # Add body to email
    message.attach(MIMEText(body, "html"))

    text = message.as_string()

    # Log in to server using secure context and send email
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        server.login(sender_email, password)
        return server.sendmail(sender_email, receiver_email, text)
