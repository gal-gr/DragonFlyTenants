
class group:
    def __init__(self,radix):
        self.radix = radix
        self.avaliableHosts= (radix*radix)/4
        self.hosts = [-1 for i in range(self.avaliableHosts)]
        self.longLinks = [ [-1 for i in range(radix/2)] for j in range(radix/2)]
        self.shortLinks = [[-1 for i in range(radix / 2)] for j in range((radix / 2))]

    def is_Single_Spine_Placed(self,numOfHosts,tennantId):
        #check for a single leaf for a tennant
        leavesCounters= self.getLeavesHosts()
        for i in range(self.radix/2):
            if leavesCounters[i] >= numOfHosts :
                self.fillLeafWithHosts(i,tennantId,numOfHosts)
                return True

        for j in range(self.radix/2):
            spine = self.shortLinks[j]
            counter = 0
            for i in range(self.radix/2):
                if spine[i] == -1:
                    counter += leavesCounters[i]
            if counter >= numOfHosts:
                self.shortLinks[j] = self.fillSpineWithHosts(spine,tennantId,numOfHosts)
                return True

        return False

    def get_num_Avaliable_hosts_Per_Spine(self,spine):

        leavesCounters = self.getLeavesHosts()
        spine_array = self.shortLinks[spine]
        counter = 0
        for i in range(self.radix / 2):
            if spine_array[i] == -1:
                counter += leavesCounters[i]
        return counter



    # maybe an error- too many +=1 on leaves found
    def getLeavesHosts(self):
        leavesCounters =[ 0 for i in range((self.radix)/2) ]
        for i in range((self.radix*self.radix)/4) :
            if(self.hosts[i] == -1):
              leavesCounters[i/((self.radix)/2)] += 1


        return leavesCounters

    def fillLeafWithHosts(self,leafId,tennantId,hostsNum):
        fillCounter = 0
        for i in range((((self.radix)/2) *leafId),(((self.radix)/2) *(leafId+1))):
            if self.hosts[i] == -1:
                self.hosts[i]= tennantId
                self.avaliableHosts -= 1
                fillCounter +=1

            if fillCounter == hostsNum:
                break

        if fillCounter != hostsNum:
            print("hosts filling error")

#TODO: check that spine array is really changed
    def fillSpineWithHosts(self, spine, tennantId, hostsNum):
        fillCounter = 0
        for i in range(self.radix/2):
            if spine[i] == -1:
                for j in range(self.radix/2):
                    if self.hosts[(i*self.radix/2)+j] ==-1:
                        self.hosts[(i * self.radix / 2) + j] = tennantId
                        self.avaliableHosts -= 1
                        spine[i] = tennantId
                        fillCounter += 1
                        if fillCounter == hostsNum:
                            return spine



        if fillCounter != hostsNum:
            print("hosts filling error")

    def fillSpineidWithHosts(self, spine_id, tennantId, hostsNum):
        fillCounter = 0
        for i in range(self.radix / 2):
            if self.shortLinks[spine_id][i] == -1:
                for j in range(self.radix / 2):
                    if self.hosts[(i * self.radix / 2) + j] == -1:
                        self.hosts[(i * self.radix / 2) + j] = tennantId
                        self.avaliableHosts -= 1
                        self.shortLinks[spine_id][i] = tennantId
                        fillCounter += 1
                        if fillCounter == hostsNum:
                           return True

        if fillCounter != hostsNum:
            print("hosts filling error")
            return False




    def getNumOfLeavesInGroup(self,tennantId):
        counter = 0
        for i in range(self.radix/2):
            for j in range(self.radix/2):
                if self.hosts[(i*self.radix/2) + j] == tennantId:
                    counter +=1
                    break
        return counter


    def getNumOfSpinesInGroup(self,tennantId):
        for i in range(self.radix/2):
            for j in range(self.radix/2):
                if self.shortLinks[i][j] == tennantId:
                    return 1

        return 0
















