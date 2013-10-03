f = open("zips.txt")
myZips = [ url for url in f.readlines()]
myUrls = [ 'http://www.melissadata.com/lookups/TaxZip.asp?Zip=' 
+ z.strip() +
'&submit1=Submit' for z in myZips]
print myUrls