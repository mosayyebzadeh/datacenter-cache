
def insert_osd_map(osdMap, name, candidates, dirty):
  osdMap.loc[name] = [candidates, dirty]


def delete_osd_map(osdMap, key):
  print('delete_before', key)
  candidates = osdMap[osdMap.index.str.contains(key)]
#  temp = self.df.loc[self.df.index.str.contains(key)]
  print('delete_osd', candidates)
