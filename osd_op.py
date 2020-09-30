
def insert_osd_map(osdMap, name, candidates, dirty):
  osdMap.loc[name] = [candidates, dirty]


