
def insert_osd_map(osdMap, name, candidates, dirty):
  osdMap.loc[name] = [candidates, dirty]


def delete_osd_map(osdMap, key):
  candidates = osdMap[osdMap.index.str.contains(key)]
  osdMap.drop(candidates.index, inplace = True) 
