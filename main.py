import collections
import numpy
import PIL.Image

path = r"C:\Users\richp\Downloads\image.png"
img = PIL.Image.open(path)
count = collections.defaultdict(int)
total = 0
for row in numpy.array(img):
    for val in row:
        tv = tuple(val)
        if tv == (255, 255, 255, 255) or tv == (216, 216, 216, 255):
            continue
        count[tv] += 1
        total += 1

for k, v in sorted(count.items(), key=lambda item: item[1]):
    print(k, v/total*100)
