import psutil
 
# Calling psutil.cpu_precent() for 4 seconds
print('The CPU usage is: ', psutil.cpu_percent(interval=0.001, percpu=True))

print('# of CPUs: ', psutil.cpu_count())
print('# of real CPUs: ', psutil.cpu_count(logical=False))
