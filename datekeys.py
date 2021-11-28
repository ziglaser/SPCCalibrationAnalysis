sdate = date(2004, 1, 1)   # start date
edate = date.today()   # end date

delta = edate - sdate       # as timedelta
print("he")
date_keys = []
for i in range(delta.days + 1):
    day = sdate + timedelta(days=i)

    # date format: mm-dd-yyyy
    format = "%Y%m%d"
    # format datetime using strftime()
    date_keys.append(day.strftime(format))