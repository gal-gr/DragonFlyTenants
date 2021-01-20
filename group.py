
class Group:


    def __init__(self,radix):
        self.avaliableHosts= (radix*radix)/4
        self.hosts = [-1 for i in range(self.avaliableHosts)]
        self.longLinks = [ [-1 for i in range(radix/2)] for j in range((radix/2)+1)]
        self.shortLinks = [[-1 for i in range(radix / 2)] for j in range((radix / 2))]








