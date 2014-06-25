import requests
import settings
import sys
from BeautifulSoup import BeautifulStoneSoup
from datetime import date, datetime
import MySQLdb

# import httplib, logging
# httplib.HTTPConnection.debuglevel = 1
# logging.basicConfig() 
# logging.getLogger().setLevel(logging.DEBUG)
# requests_log = logging.getLogger("requests.packages.urllib3")
# requests_log.setLevel(logging.DEBUG)
# requests_log.propagate = True

LOGIN_URL = "https://login.opendns.com"
DASHBOARD_URL = "https://dashboard.opendns.com"

run_date = datetime.strptime(sys.argv[1], '%Y-%m-%d').strftime('%Y-%m-%d')

# Grab a form token
session = requests.Session()
token_response = session.get(LOGIN_URL)
if token_response.status_code != requests.codes.ok:
	sys.stderr.write("Could not fetch form token\n")
	sys.exit(1)
soup = BeautifulStoneSoup(token_response.text)
token_el = soup.findAll(attrs={"name" : "formtoken"})
form_token = token_el[0]['value']

# Log into the portal
login_response = session.post(LOGIN_URL, allow_redirects=False, data={
	"formtoken": form_token,
	"username": settings.USERNAME,
	"password": settings.PASSWORD,
	"sign-in": "Sign in",
	"return_to": "https://dashboard.opendns.com/"
}, headers = {
	"Content-Type": "application/x-www-form-urlencoded",
	"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/34.0.1847.116 Safari/537.36"
})
if login_response.status_code != 302:
	sys.stderr.write("Could not log in\n")
	sys.exit(1)

more = True
page = 1
domains = []
domains_in_batch = 0
while more:
	url = "{DASHBOARD_URL}/stats/{NETWORK_ID}/topdomains/{DATE}/page{PAGE}.csv".format(
		DASHBOARD_URL=DASHBOARD_URL,
		NETWORK_ID=settings.NETWORK_ID,
		DATE=run_date,
		PAGE=page
	)

	stats_response = session.get(url, allow_redirects=False)
	if stats_response.status_code != requests.codes.ok:
		print stats_response.status_code
		more = False
	for line in stats_response.text.split("\n")[1:]:
		if line != '':
			domains.append(line.split(',')[1])
			domains_in_batch += 1
	if domains_in_batch == 0:
		more = False
	domains_in_batch = 0
	page += 1

db = MySQLdb.connect(**settings.DATABASE) 
cursor = db.cursor() 

# Delete any data that might already exist for this day
cursor.execute("DELETE FROM domains WHERE `date` = %s", [run_date])

# Insert domains into table
parameters = []
query = "INSERT INTO domains (`date`, `domain`) VALUES (%s, %s)"
for domain in domains:
	parameters.append((run_date, domain,))
print cursor.executemany(query, parameters)

# Close all databases
db.commit()
cursor.close()
db.close()

# Report status
print "Backup of data complete"