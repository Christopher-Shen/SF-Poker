import gspread
from oauth2client.service_account import ServiceAccountCredentials

scope: list = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]
creds: ServiceAccountCredentials = ServiceAccountCredentials.from_json_keyfile_name(
    "SF_Poker_Secrets.json", scope
)
client = gspread.authorize(creds)

sheet: dict = client.open("SF Poker").sheet1

test_sheet = sheet.get_all_records()
print(test_sheet)
