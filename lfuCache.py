#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""

"""
class CacheNode(object):
    def __init__(self, key, value, freq_node, pre, nxt):
        self.key = key
        self.value = value
        self.freq_node = freq_node
        self.pre = pre  # previous CacheNode
        self.nxt = nxt  # next CacheNode

    def free_myself(self):
        if self.freq_node.cache_head == self.freq_node.cache_tail:
            self.freq_node.cache_head = self.freq_node.cache_tail = None
        elif self.freq_node.cache_head == self:
            self.nxt.pre = None
            self.freq_node.cache_head = self.nxt
        elif self.freq_node.cache_tail == self:
            self.pre.nxt = None
            self.freq_node.cache_tail = self.pre
        else:
            self.pre.nxt = self.nxt
            self.nxt.pre = self.pre

        self.pre = None
        self.nxt = None
        self.freq_node = None


class FreqNode(object):
    def __init__(self, freq, pre, nxt):
        self.freq = freq
        self.pre = pre  # previous FreqNode
        self.nxt = nxt  # next FreqNode
        self.cache_head = None  # CacheNode head under this FreqNode
        self.cache_tail = None  # CacheNode tail under this FreqNode

    def count_caches(self):
        if self.cache_head is None and self.cache_tail is None:
            return 0
        elif self.cache_head == self.cache_tail:
            return 1
        else:
            return '2+'

    def remove(self):
        if self.pre is not None:
            self.pre.nxt = self.nxt
        if self.nxt is not None:
            self.nxt.pre = self.pre

        pre = self.pre
        nxt = self.nxt
        self.pre = self.nxt = self.cache_head = self.cache_tail = None

        return (pre, nxt)

    def pop_head_cache(self):
        if self.cache_head is None and self.cache_tail is None:
            return None
        elif self.cache_head == self.cache_tail:
            cache_head = self.cache_head
            self.cache_head = self.cache_tail = None
            return cache_head
        else:
            cache_head = self.cache_head
            self.cache_head.nxt.pre = None
            self.cache_head = self.cache_head.nxt
            return cache_head

    def append_cache_to_tail(self, cache_node):
        cache_node.freq_node = self

        if self.cache_head is None and self.cache_tail is None:
            self.cache_head = self.cache_tail = cache_node
        else:
            cache_node.pre = self.cache_tail
            cache_node.nxt = None
            self.cache_tail.nxt = cache_node
            self.cache_tail = cache_node

    def insert_after_me(self, freq_node):
        freq_node.pre = self
        freq_node.nxt = self.nxt

        if self.nxt is not None:
            self.nxt.pre = freq_node

        self.nxt = freq_node

    def insert_before_me(self, freq_node):
        if self.pre is not None:
            self.pre.nxt = freq_node

        freq_node.pre = self.pre
        freq_node.nxt = self
        self.pre = freq_node


class LFUCache(object):

    def __init__(self, capacity):
        self.cache = {}  # {key: cache_node}
        self.capacity = capacity
        self.freq_link_head = None

    def get(self, key):
        if key in self.cache:
            cache_node = self.cache[key]
            freq_node = cache_node.freq_node
            value = cache_node.value

            self.move_forward(cache_node, freq_node)

            return value
        else:
            return -1

    def getAging(self, key, aging, cacheServer, dc):
        #if key in self.cache:
            cache_node = self.cache[key]
            freq_node = cache_node.freq_node
            value = cache_node.value
            cacheServer.avgFreq -= freq_node.freq
            self.move_forwardAging(cache_node, freq_node, aging)

            cache_node = self.cache[key]
            freq_node = cache_node.freq_node
            cacheServer.avgFreq += freq_node.freq
            #print(key, freq_node.freq, dc.blk_dir.dict[key]['gfreq']+1, max(freq_node.freq, dc.blk_dir.dict[key]['gfreq']+1))
            dc.blk_dir.updateGFreqLFUDA(key, max(freq_node.freq, dc.blk_dir.dict[key]['gfreq']+1))

            #d = dc.blk_dir.dict
            #for k in d.keys():
            #    print(k, d[k]['gfreq'], d[k]['location'])
            return value
        #else:
        #    return -1


    def set(self, key, value):
        if self.capacity <= 0:
            return -1

        dumped_cache_node = ""
        if key not in self.cache:
            if len(self.cache) >= self.capacity:
                dumped_cache_node = self.dump_cache()

            self.create_cache(key, value)
            if dumped_cache_node:
                #return [dumped_cache_node[0], dumped_cache_node[1], dumped_cache_node[2]]
                return dumped_cache_node
            else:
                return
        else:
            cache_node = self.cache[key]
            freq_node = cache_node.freq_node
            cache_node.value = value

            self.move_forward(cache_node, freq_node)
            return

    def setAging(self, key, value, aging, cacheServer, dc):
        if self.capacity <= 0:
            return -1

        dumped_cache_node = None
        if key not in dc.blk_dir.dict: #no copy
            newObject = 1
            #print("setaging: new object")
            if len(self.cache) >= self.capacity:
                dumped_cache_node = self.dump_cacheAging(key, dc, newObject)
                #print("setaging: newobject dumped object is: ", dumped_cache_node[0])

                if dumped_cache_node is None:
                    dumped_cache_node = self.dump_cache()
                    #print("setaging: newobject dumped object is null: ", dumped_cache_node[0])

                cacheServer.avgFreq -= dumped_cache_node[2]
                aging = max(aging, dumped_cache_node[2])
                #print("setaging: aging is: ", aging)

                #dumped_cache_node_key = dumped_cache_node.key
                #dumped_cache_node_value = dumped_cache_node.value
                #dumped_cache_node_freq = dumped_cache_node.freq_node.freq

                self.create_cacheAging(key, value, aging)
    
                cache_node = self.cache[key]
                freq_node = cache_node.freq_node
                cacheServer.avgFreq += freq_node.freq
                dc.blk_dir.put(key, value, cacheServer.name)
                dc.blk_dir.updateGFreqLFUDA(key, freq_node.freq)


                #return [dumped_cache_node_key, dumped_cache_node_value, dumped_cache_node_freq]
                #return [dumped_cache_node[0], dumped_cache_node[1], dumped_cache_node[2]]
                return dumped_cache_node
            else:
                self.create_cacheAging(key, value, aging)

                cache_node = self.cache[key]
                freq_node = cache_node.freq_node
                cacheServer.avgFreq += freq_node.freq
                dc.blk_dir.put(key, value, cacheServer.name)
                dc.blk_dir.updateGFreqLFUDA(key, freq_node.freq)
                return

        elif key not in self.cache:
          if len(self.cache) >= self.capacity:
            newObject = 0
            dumped_cache_node = self.dump_cacheAging(key, dc, newObject)
            if dumped_cache_node is None:
                return
            cacheServer.avgFreq -= dumped_cache_node[2]

            self.create_cacheAging(key, value, aging)

            cache_node = self.cache[key]
            freq_node = cache_node.freq_node
            cacheServer.avgFreq += freq_node.freq
            dc.blk_dir.put(key, value, cacheServer.name)
            #dc.blk_dir.updateGFreqLFUDA(key, freq_node.freq)
            dc.blk_dir.updateGFreqLFUDA(key, max(dc.blk_dir.dict[key]['gfreq'], freq_node.freq))

            #return victim_node
            #print("candid and victim have copy, evicting the victim", victim_node_key, victim_node_value, victim_node_freq)
            if dumped_cache_node is not None:
                #return [dumped_cache_node[0], dumped_cache_node[1], dumped_cache_node[2]]
                return dumped_cache_node
            else:
                return

          else:
            self.create_cacheAging(key, value, aging)

            cache_node = self.cache[key]
            freq_node = cache_node.freq_node
            cacheServer.avgFreq += freq_node.freq
            dc.blk_dir.put(key, value, cacheServer.name)
            dc.blk_dir.updateGFreqLFUDA(key, max(dc.blk_dir.dict[key]['gfreq'], freq_node.freq))

        else:
            cache_node = self.cache[key]
            freq_node = cache_node.freq_node
            cacheServer.avgFreq -= freq_node.freq
            cache_node.value = value

            self.move_forwardAging(cache_node, freq_node, aging)

            cache_node = self.cache[key]
            freq_node = cache_node.freq_node
            cacheServer.avgFreq += freq_node.freq
            #dc.blk_dir.updateGFreqLFUDA(key, freq_node.freq)
            dc.blk_dir.updateGFreqLFUDA(key, max(dc.blk_dir.dict[key]['gfreq'], freq_node.freq))
            return



    def move_forward(self, cache_node, freq_node):
        if freq_node.nxt is None or freq_node.nxt.freq != freq_node.freq + 1:
            target_freq_node = FreqNode(freq_node.freq + 1, None, None)
            target_empty = True
        else:
            target_freq_node = freq_node.nxt
            target_empty = False

        cache_node.free_myself()
        target_freq_node.append_cache_to_tail(cache_node)

        if target_empty:
            freq_node.insert_after_me(target_freq_node)

        if freq_node.count_caches() == 0:
            if self.freq_link_head == freq_node:
                self.freq_link_head = target_freq_node

            freq_node.remove()

    def move_forwardAging(self, cache_node, freq_node, aging):
        base_freq = freq_node.freq
        base_freq_node = freq_node
        while freq_node.nxt is not None and freq_node.nxt.freq < base_freq + aging + 1:
            freq_node = freq_node.nxt

        target_freq_node = FreqNode(0, None, None)
        target_empty = True
        if freq_node.nxt is None:
            target_freq_node = FreqNode(base_freq + aging + 1, None, None)
            cache_node.free_myself()
            target_freq_node.append_cache_to_tail(cache_node)
            freq_node.insert_after_me(target_freq_node)
            #target_empty = True
        if freq_node.nxt.freq > base_freq + aging + 1:
            target_freq_node = FreqNode(base_freq + aging + 1, None, None)
            cache_node.free_myself()
            target_freq_node.append_cache_to_tail(cache_node)
            freq_node.insert_after_me(target_freq_node)
        elif freq_node.nxt.freq == base_freq + aging + 1:
            target_freq_node = freq_node.nxt
            cache_node.free_myself()
            target_freq_node.append_cache_to_tail(cache_node)


        freq_node = base_freq_node
        if freq_node.count_caches() == 0:
            if self.freq_link_head == freq_node:
                self.freq_link_head = freq_node.nxt

            freq_node.remove()



    def dump_cache(self):
        head_freq_node = self.freq_link_head
        cache_node = head_freq_node.cache_head

        victim_node_key = cache_node.key
        victim_node_value = cache_node.value
        victim_node_freq = cache_node.freq_node.freq

        self.cache.pop(head_freq_node.cache_head.key)
        head_freq_node.pop_head_cache()


        if head_freq_node.count_caches() == 0:
            self.freq_link_head = head_freq_node.nxt
            head_freq_node.remove()
        return [victim_node_key, victim_node_value, victim_node_freq]

    def dump_cacheAging(self, key, dc, newObject):
      victim_node = None
      head_freq_node = self.freq_link_head
      if newObject == 1:
        victim_node = head_freq_node.cache_head
        #print("dump_cacheAging", victim_node.key, dc.blk_dir.dict[victim_node.key]['location'])
        count = 0
        found  = 1
        while len(dc.blk_dir.dict[victim_node.key]['location']) == 1:
            #print("dump_cacheAging loop", victim_node.key, dc.blk_dir.dict[victim_node.key]['location'])
            if count >= 5: #read the first 5 victims
                found = 0
                break
            #print("set aging key not in local, victim is: ", victim_node.key )
            if victim_node.nxt:
                victim_node = victim_node.nxt
                #print("loop next victim ", victim_node.key )
                count += 1
            else: #candid has copy, victim has no copy. do Nothing
                #print("victim has no copy")
                found = 0
                break
 
        if found == 0:
            #print("not found")
            minFrq = 10000000
            victim = head_freq_node.cache_head
            #print("next victim ", victim.key )
            for i in range(5):
                if dc.blk_dir.dict[victim.key]["gfreq"] < minFrq:
                    minFrq = dc.blk_dir.dict[victim.key]["gfreq"]
                    victim_node = victim
                if victim.nxt:
                    victim = victim.nxt
                else:
                    victim = head_freq_node.cache_head
                    break

            #print("victim and its gfreq is: ", victim_node.key, dc.blk_dir.dict[victim_node.key]['gfreq'])

      elif newObject == 0:
            victim = head_freq_node.cache_head
            for i in range(5):
                if dc.blk_dir.dict[victim.key]["gfreq"] < dc.blk_dir.dict[key]["gfreq"]:
                    victim_node = victim
                    break
                elif victim.nxt:
                    victim = victim.nxt
            if victim_node == None:
                return


      #aging = max(aging, head_freq_node.freq)
      #print(self.cache.keys())
      self.cache.pop(victim_node.key)
      #print(self.cache.keys())
      victim_node_key = victim_node.key
      victim_node_value = victim_node.value
      victim_node_freq = victim_node.freq_node.freq
      victim_node.free_myself()

      #head_freq_node.pop_head_cache()
      if head_freq_node.count_caches() == 0:
          self.freq_link_head = head_freq_node.nxt
          head_freq_node.remove()
        
      return [victim_node_key, victim_node_value, victim_node_freq]


    def create_cache(self, key, value):
        cache_node = CacheNode(key, value, None, None, None)
        self.cache[key] = cache_node

        if self.freq_link_head is None or self.freq_link_head.freq != 0:
            new_freq_node = FreqNode(0, None, None)
            new_freq_node.append_cache_to_tail(cache_node)

            if self.freq_link_head is not None:
                self.freq_link_head.insert_before_me(new_freq_node)

            self.freq_link_head = new_freq_node
        else:
            self.freq_link_head.append_cache_to_tail(cache_node)


    def create_cacheAging(self, key, value, aging):
        cache_node = CacheNode(key, value, None, None, None)
        self.cache[key] = cache_node

        if self.freq_link_head is None:
            #print("create_cacheAging: new header" )
            new_freq_node = FreqNode(aging, None, None)
            new_freq_node.append_cache_to_tail(cache_node)

            self.freq_link_head = new_freq_node
            return
        
        freq_node = self.freq_link_head
        header_freq = freq_node.freq
        #print("create_cacheAging:", freq_node.freq)

        """
        if freq_node.freq == aging:
            target_freq_node = freq_node
            target_freq_node.append_cache_to_tail(cache_node)
            return

        elif freq_node.freq > aging:
            new_freq_node = FreqNode(aging, None, None)
            new_freq_node.append_cache_to_tail(cache_node)
            self.freq_link_head.insert_before_me(new_freq_node)
            return
        """

        while freq_node.nxt is not None and freq_node.nxt.freq < aging:
            freq_node = freq_node.nxt

        #print("create_cacheAging:", freq_node.freq)
        target_freq_node = FreqNode(0, None, None)
        target_empty = True

        #if freq_node.freq == header_freq + aging:
        if freq_node.freq == aging:
            #print("create_cacheAging: current node is equal")
            target_freq_node = freq_node
            target_freq_node.append_cache_to_tail(cache_node)
        elif freq_node.freq > aging:
            #print("create_cacheAging: current node is bigger")
            target_freq_node = FreqNode(aging, None, None)
            target_freq_node.append_cache_to_tail(cache_node)
            freq_node.insert_before_me(target_freq_node)
            self.freq_link_head = target_freq_node
        elif freq_node.nxt is None:
            #print("create_cacheAging: next node is none")
            target_freq_node = FreqNode(aging, None, None)
            target_freq_node.append_cache_to_tail(cache_node)
            freq_node.insert_after_me(target_freq_node)
        elif freq_node.nxt.freq > aging:
            #print("create_cacheAging: next node has higher freq" )
            target_freq_node = FreqNode(aging, None, None)
            target_freq_node.append_cache_to_tail(cache_node)
            freq_node.insert_after_me(target_freq_node)
        elif freq_node.nxt.freq == aging:
            #print("create_cacheAging: next node is equal")
            target_freq_node = freq_node.nxt
            target_freq_node.append_cache_to_tail(cache_node)



