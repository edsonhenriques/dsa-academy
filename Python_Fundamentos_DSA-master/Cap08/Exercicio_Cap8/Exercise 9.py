import pandas as pd
import requests
import matplotlib.pyplot as plt

url = 'https://api.github.com/repos/pandas-dev/pandas/issues'
resp = requests.get(url)
data = resp.json()
print(data[0]['title'])
issues = pd.DataFrame(data, columns=['number', 'title', 'labels', 'state'])
print(issues)
ax = plt.subplot(111, frame_on=False)
plt.savefig('mytable.png')
plt.show()

"""ax = plt.subplot(111, frame_on=False) # no visible frame
ax.xaxis.set_visible(False)  # hide the x axis
ax.yaxis.set_visible(False)  # hide the y axis

table(ax, df)  # where df is your data frame

plt.savefig('mytable.png')"""