import time
from termcolor import colored

class Delay:
    """Delay on-off Function Block"""
    def __init__(self, delay_on=0, delay_off=0, verbose=False):
        self.delay_on = delay_on
        self.delay_off = delay_off
        self.ctime = 0                     # change signal time
        self.isig = None                   # previous input signal
        self.osig = False                  # current output signal
        self.__verbose = verbose
        
    def _verbose(self, *st):
        if self.__verbose:
            print(*st)
        
    def output(self, signal, changed=False, dtime=0):
        if changed:
            self._verbose("*$* Time changed! *$*")
            self.ctime = dtime if dtime else time.time()

        self.osig = signal
        
        color = "green" if self.isig == self.osig else "grey"
        on_c = "on_white" if self.isig == self.osig else "on_red"   
        conn = " ---------- " if self.isig == self.osig else " ----/---- " 
        self._verbose(colored(str(self.isig)+conn+str(self.osig), color, on_c, attrs=['bold']))
        
        return signal
        
        
    def _inner(self, signal, dtime=0):
        delay = dtime - self.ctime if dtime else time.time() - self.ctime
        self._verbose(f"{float(delay):.3f}, input sig:{signal}, dtime: {dtime}")
        assert signal in [True, False], "Signal must be bool value!"
        
        if (signal==True and self.delay_on==0) or (signal==False and self.delay_off==0):
            ct = (self.isig != signal)
            self.isig = signal
            return self.output(signal, ct, dtime)

        if signal==self.isig and delay>=self.delay_off and signal==False:
            return self.output(signal)
        elif signal==False:
            ct = (self.isig != signal)
            self.isig = signal
            return self.output(self.osig, ct, dtime) 
        
        if signal==self.isig and delay>=self.delay_on and signal==True:
            return self.output(signal)
        elif signal==True:
            ct = (self.isig != signal)
            self.isig = signal
            return self.output(self.osig, ct, dtime) 
            
            
    def __call__(self, sig, dtime=0):
        return self._inner(sig, dtime)





if __name__ == "__main__":
    dele = Delay(delay_on=2, delay_off=0, verbose=True)
    
    
    print("\n\n$$$$$$$$$$$\     ONLINE MODE      $$$$$$$$$$$\n\n")
    # Test Delay in online mode
    insec = 2
    rep = 2

    l = []
    l += [False]*10
    l += ([True]*insec + [False]*insec) * rep
    l += ([True]*2*insec + [False]*2*insec) * rep
    l += ([True]*3*insec + [False]*3*insec) * rep
    l += ([True]*insec + [False]*insec) * rep
    l += ([True]*4*insec + [False]*4*insec) * rep
    l += ([True]*5*insec + [False]*5*insec) * rep
    l += ([True]*6*insec + [False]*6*insec) * rep


    for c in l:
        time.sleep(0.4)
        inp = c
        out = dele(c)
        
        print("$$"*20)
        print()


    print("\n\n$$$$$$$$$$$     OFFLINE MODE      $$$$$$$$$$$\n\n")
    ##   Test Delay in offline mode  (data with time)
    from random import randrange, choice, seed
    import datetime 

    seed(123456)

    prev = False

    def sig_gen(prev):
        c = [prev, True, False]
        return choice(c)


    def random_date(start,l):
        current = start
        while l >= 0:
            tdl = datetime.timedelta(seconds=randrange(1,7))
            #print(tdl)
            current = current + tdl
            yield current
            l-=1


    data = []
    startDate = datetime.datetime(2019, 9, 20,13,00)

    for x in random_date(startDate,20):
        prev = sig_gen(prev)
        data.append((x, prev))


    dl = Delay(delay_on=3, delay_off=4)
    for t, d in data:
        inp = d
        out = dl(d, t.timestamp())
        color = "green" if inp == out else "grey"
        on_c = "on_white" if inp == out else "on_red"   
        conn = " ---------- " if inp == out else " ----/---- " 
        print(colored(str(d) + conn + str(out) , color, on_c, attrs=['bold']))
        print("$$"*20)
        print()
    
