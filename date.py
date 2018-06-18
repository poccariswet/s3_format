from datetime import datetime, timedelta

start = datetime.strptime('20161115', '%Y%m%d')
end = datetime.strptime('20180615', '%Y%m%d')

def daterange(start_date, end_date):
    for n in range((end_date - start_date).days):
        yield start_date + timedelta(n)

dates = []
for i in daterange(start, end):
    y = str(i.year)
    m = str(i.month)
    d = str(i.day)
    if len(m) !=2:
        m = '0' + m
    if len(d) != 2:
        d = '0' + d

    dates.append(y+m+d)


print(len(dates))

for date in dates:
    print(date)
