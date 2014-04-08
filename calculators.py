from __future__ import division


def bmaist(arg):
    return arg["temperature"] + arg["pressure"]
    #return 1


def ctof(arg):
    return (arg["centigrade"]*1.8)+32

def ftok(arg):
    return (arg["farenheit"]+459.67)*(5/9)
