#!/usr/bin/env python3

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import pandas as pd
from datetime import datetime
import numpy as np
import os
import smtplib
from email.message import EmailMessage
from dotenv import load_dotenv
from pathlib import Path
import logging
import time

# Load environment variables
load_dotenv()
ScriptDir = Path(__file__).parent
SERVICE_ACCOUNT_FILE = ScriptDir / "service_account.json"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
GSheetID = os.getenv('GSheetID') # Google Sheet ID
JobName = os.getenv('JobName') # Name of the sheet/tab

# Setup logging
LogDir = ScriptDir / f"{JobName}Logs"
LogDir.mkdir(exist_ok=True) # create log folder if it doesn't exist
timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
LogFile = LogDir / f"log_{timestamp}.txt"

logging.basicConfig(
    filename=LogFile,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
start_time = time.time()
logging.info("Script started")


# Connect to Google Sheets
creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
service = build("sheets", "v4", credentials=creds, cache_discovery=False)

# Fetch sheet values
RangeName = f"{JobName}!A1:I"
result = service.spreadsheets().values().get(
    spreadsheetId=GSheetID,
    range=RangeName
).execute()
rows = result.get("values", [])

# Build DataFrame, use first row as headers
df = pd.DataFrame(rows[1:], columns=rows[0])
df = df.dropna(how='all') # remove completely empty rows
df['Issue Date'] = pd.to_datetime(df['Issue Date'].astype(str).str.strip(), dayfirst=True, errors='coerce')                               
df['Due Date']   = pd.to_datetime(df['Due Date'].astype(str).str.strip(), dayfirst=True, errors='coerce')
today = datetime.today().date()
today = pd.Timestamp('today').normalize()
df = df.sort_values(by='Issue Date', ascending=False)
df_latest = df.drop_duplicates(subset=['Machinery', 'System'], keep='first').copy()
df_latest['Days Until Expiration'] = (df_latest['Due Date'].dt.normalize() - today).dt.days

# Define expiration codes
# Expiration status codes: 0=Not Expired, 1=Near, 2=Almost, 3=Expired, 4=Expired over a week
def expiration_code(days):
    if days < -6:
        return 4
    elif days < 8:
        return 3
    elif days < 15:
        return 2
    elif days < 31:
        return 1
    else:
        return 0

df_latest['Updated Notification'] = df_latest['Days Until Expiration'].apply(expiration_code)
df_latest['Notification'] = df_latest['Notification'].replace('', np.nan).fillna(0).astype(int)
status_changed = df_latest[df_latest['Notification'] != df_latest['Updated Notification']]
status_changed = status_changed.sort_values(by='Days Until Expiration')

if not status_changed.empty:
    # Email setup
    FromMail = os.getenv('FromMail')
    AppPWS = os.getenv('AppPWS')

    to_notify = df_latest[df_latest['Updated Notification'] > 0].copy()
    to_notify['Changed'] = to_notify.index.isin(status_changed.index)

    # Build HTML email body
    email_body = """
    <html>
    <body>
    <p>Dear Team,</p>
    <p>Please find below the certification notifications:</p>

    <table style="border-collapse: collapse; border: 1px solid black; width: 100%;">
        <tr style="background-color: #f2f2f2;">
            <th style="border: 1px solid black; padding: 6px; text-align:center;">Remaining Days</th>
            <th style="border: 1px solid black; padding: 6px; text-align:center;">Machinery</th>
            <th style="border: 1px solid black; padding: 6px; text-align:center;">System</th>
            <th style="border: 1px solid black; padding: 6px; text-align:center;">Due Date</th>
            <th style="border: 1px solid black; padding: 6px; text-align:center;">Curent Cert. Type</th>
            <th style="border: 1px solid black; padding: 6px; text-align:center;">Certification No.</th>
            <th style="border: 1px solid transparent; background-color:white; padding: 6px; text-align:center;"></th>
        </tr>
    """
    # Fill table rows
    for _, row in to_notify.iterrows():
        # Determine row background color by notification level
        level = row['Updated Notification']
        if level == 4:
            row_style = 'background-color: #b22222;'  # dark red
        elif level == 3:
            row_style = 'background-color: #f8d7da;'  # light red
        elif level == 2:
            row_style = 'background-color: #fff3cd;'  # light yellow
        else:
            row_style = ''

        NewCheck= status_changed.shape[0] == to_notify.shape[0]
        if not NewCheck:
            if row['Changed']:
                if level == 1:
                    change_text = '<span style="color:red;">New</span>'
                else:
                    change_text = '<span style="color:red;">Updated</span>'
            else:
                change_text = ''
        else:
            change_text = ''


        email_body += f"""
        <tr style="{row_style}">
            <td style="border: 1px solid black; padding: 6px; text-align:center;">{row['Days Until Expiration']}</td>
            <td style="border: 1px solid black; padding: 6px; text-align:center;">{row['Machinery']}</td>
            <td style="border: 1px solid black; padding: 6px; text-align:center;">{row['System']}</td>
            <td style="border: 1px solid black; padding: 6px; text-align:center;">{row['Due Date'].strftime('%d/%m/%Y') if not pd.isna(row['Due Date']) else ''}</td>
            <td style="border: 1px solid black; padding: 6px; text-align:center;">{row['Type']}</td>
            <td style="border: 1px solid black; padding: 6px; text-align:center;">{row['Certification No.']}</td>
            <td style="border:1px solid transparent; background-color:white; padding:6px;">
                <div style="color:red; font-weight:normal;">{change_text}</div>
            </td>
        </tr>
        """

    email_body += """
    </table>
    <p>Your Automation Script</p>
    </body>
    </html>
    """

    # Prepare email
    msg = EmailMessage()
    msg['Subject'] = f'Ενημέρωση Πιστοποιητικών Μηχανημάτων - {today.strftime('%d/%m/%Y')}'
    msg['From'] = FromMail
    SendTo = os.getenv('SendTo')
    msg['To'] = ', '.join([email.strip() for email in SendTo.split(',')])
    msg.set_content("Το email απαιτεί πρόγραμμα ανάγνωσης HTML.")
    msg.add_alternative(email_body, subtype='html')

    try:
        # Send email
        with smtplib.SMTP('smtp.gmail.com', 587) as smtp:
            smtp.starttls()
            smtp.login(FromMail, AppPWS)
            smtp.send_message(msg)

        NotifCerts = ", ".join(f"{row['Machinery']}|{row['System']}" for _, row in status_changed.iterrows())
        logging.info(f"Email sent to: {SendTo} regarding: {NotifCerts}")

        # Update Google Sheet notifications
        for idx in status_changed.index:
            sheet_row = idx + 2 # add 2 to account for header row in sheet
            range = f"{JobName}!A{sheet_row}"
            value = [[int(status_changed.loc[idx, 'Updated Notification'])]]
            body = {"values": value}

            try:
                service.spreadsheets().values().update(
                    spreadsheetId=GSheetID,
                    range=range,
                    valueInputOption="RAW",
                    body=body
                ).execute()
                mach = status_changed.loc[idx, 'Machinery']
                system = status_changed.loc[idx, 'System']
                logging.info("Spreadsheet updated successfully | R:%d M:%s S:%s", sheet_row, mach, system)

            except Exception as e:
                logging.error(f"Failed to update spreadsheet: {e}")
                raise
       
    except Exception as e:
        logging.error(f"Failed to send email: {str(e)}")

else:
    logging.info("No changes detected. No email sent")

end_time = time.time()
elapsed = round(end_time - start_time, 2)
logging.info(f"Script completed in {elapsed} seconds")