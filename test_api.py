import requests

data = {'token': '9F01E61ABBDAED93744274003B4E7360', 'content': 'record', 'action': 'export', 'format': 'csv', 'type': 'flat', 'csvDelimiter': '', 'rawOrLabel': 'label', 'rawOrLabelHeaders': 'label', 'exportCheckboxLabel': 'true', 'exportSurveyFields': 'true', 'exportDataAccessGroups': 'true', 'returnFormat': 'json'}


r = requests.post('https://redcap.kumc.edu/api/',data=data)
print('HTTP Status: ' + str(r.status_code))
print(r.text)
