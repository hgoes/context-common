"""
The rule system
===============
"""
import numpy as np
from math import exp
from ConfigParser import ConfigParser

class ClassifierSet:
    """
    A classifier set contains a list of classifiers and attached semantics
    """
    def __init__(self,classifiers,tp='movement',
                 semantics=["mean_x1","mean_y1","mean_z1","mean_x2","mean_y2","mean_z2","var_x1","var_y1","var_z1","var_x2","var_y2","var_z2","last"]):
        self.classifiers = [ c for c in classifiers ]
        self.type = tp
        self.semantics = semantics
    def step(self,v):
        pos = 0
        rst = None
        prob = 0
        for classifier in self.classifiers:
            (st,max,cprob) = classifier.evaluate(v)
            if st != None:
                prob = cprob
                rst = st
                #print "Classifier",classifier.name,"matched with state",st,"and value",prob
                break
            print "Classifier",classifier.name,"didn't match"
            pos += 1
        if rst is not None:
            if pos!=0:
                cl = self.classifiers.pop(pos)
                self.classifiers = [cl]+self.classifiers
        return (rst,prob)
    def dimension(self):
        return self.classifiers[0].dimension()
    def to_json(self):
        return { 'type' : self.type,
                 'semantics' : self.semantics,
                 'dimension' : self.dimension(),
                 'classifier' : [ cl.to_json() for cl in self.classifiers ]
                 }
    @staticmethod
    def from_json(node):
        dim = node['dimension']
        tp = node['type']
        sem = node['semantics']
        classifier = [ Classifier.from_json(cl,dim) for cl in node['classifier'] ]
        return ClassifierSet(classifier,tp,semantics)
    @staticmethod
    def from_ini(cfgparser):
        dims = cfgparser.getint("DEFAULT","dimensions")
        rules = []
        for rule in cfgparser.sections():
            means = np.array(map(float,cfgparser.get(rule,"mean").split()))
            var = np.array(map(float,cfgparser.get(rule,"sigma").split()))
            cons = map(float,cfgparser.get(rule,"consequence").split())
            if cfgparser.has_option(rule,"bitvec"):
                bitvec = np.array(map(bit,cfgparser.get(rule,"bitvec").split())).nonzero()
            else:
                bitvec = None
            result = np.array(cons[0:-1])
            roff = cons[-1]
            rules.append(ComplexRule(result,roff,means,var,bitvec))
        return RuleSet(rules)

def fuzzy(avg,var,x):
    return max(0,1 - abs(avg-x)/var)

class Classifier:
    def __init__(self,ruleset,memb=[],name=""):
        self.ruleset = ruleset
        self.membership = memb
        self.name = name
    def evaluate(self,v):
        g = self.ruleset.evaluate(v)
        #print "Classifier ",self.name,"produced value",g
        max = 0.5
        max_el = None
        for (k,v) in self.membership:
            #r = fuzzy(v,1.0,g)
            r = abs(v-g)
            if r < 0.5 and max > r:
                max = r
                max_el = k
        return (max_el,max,g)
    def dimension(self):
        return self.ruleset.rules[0].dimension()
    def to_json(self):
        return { 'rules' : self.ruleset.to_json(),
                 'name' : self.name,
                 'mapping' : [ { 'class' : k, 'value' : float(v) } for (k,v) in self.membership ]
                 }
    @staticmethod
    def from_json(node,dim):
        rset = RuleSet.from_json(node['rules'],dim)
        name = node['name']
        mapping = [ (nd['class'],nd['value']) for nd in node['mapping'] ]
        return Classifier(rset,mapping,name)

class RuleSet:
    def __init__(self,rules):
        self.rules = rules
    def evaluate(self,v):
        r = 0.0
        d = 0.0
        for rule in self.rules:
            w = rule.weight(v)
            e = rule.evaluate(v)
            print "Weight",w
            print "Res",e
            r += w*e
            d += w
        return r/d
    def evaluates(self,vs):
        r = np.zeros(vs.shape[0])
        d = np.zeros(vs.shape[0])
        for rule in self.rules:
            w = rule.weights(vs)
            e = rule.evaluates(vs)
            r += w*e
            d += w
        return r/d
    def __str__(self):
        res = "RuleSet: | "
        for rule in self.rules:
            res += str(rule)+" |"
        return res
    def dimension(self):
        return self.rules[0].dimension()
    def to_json(self):
        return [ rule.to_json() for rule in self.rules ]
    @staticmethod
    def from_json(node,dim):
        return RuleSet([ ComplexRule.from_json(nd,dim) for nd in node])

def bit(str):
    if str == "0":
        return 1
    else:
        return 0

class Rule:
    def __init__(self,rvec,roff,bitvec=None):
        self.rvec = rvec
        self.roff = roff
        self.bitvec = bitvec
    def evaluate(self,v):
        return np.vdot(self.rvec,v) + self.roff
    def evaluates(self,vs):
        return np.sum(vs*self.rvec,1) + self.roff
    def weight(self,v):
        t = np.array([v - self.means()])
        if self.bitvec is not None:
            t.put(self.bitvec,0)
        return exp(-0.5*(np.dot(np.dot(t,self.variance()),t.T))[0,0])
    def weights(self,vs):
        t = vs - self.means()
        return np.exp(-0.5*(np.sum(np.dot(t,self.variance())*t,1)))
    def means(self):
        abstract
    def variance(self):
        abstract
    def dimension(self):
        return self.rvec.shape[0]
    def to_json(self):
        return { 'consequence' : self.rvec.tolist() + [self.roff],
                 'sigma' : self.variance().flatten().tolist(),
                 'mean': self.means().tolist()
                 }

class SimpleRule(Rule):
    def __init__(self,rvec,roff,rules,bitvec=None):
        Rule.__init__(self,rvec,roff,bitvec)
        self.rules = rules
    def means(self):
        return np.array([m for (m,v) in self.rules])
    def variance(self):
        return np.array(np.diag([1/v for (m,v) in self.rules]))

class ComplexRule(Rule):
    """
    A representation of a rule represented by a result vector, a result offset, a mean vector and a covariance matrix.
    """
    def __init__(self,rvec,roff,vmean,covar,bitvec=None):
        Rule.__init__(self,rvec,roff,bitvec)
        self.vmean = vmean
        self.covar = covar
    def means(self):
        return self.vmean
    def variance(self):
        return self.covar
    def __str__(self):
        return "ComplexRule:\nCovar: "+str(self.covar)+"\nMeans: "+str(self.vmean)+"\nResult: "+str(self.rvec)
    @staticmethod
    def from_json(node,dim):
        sigma = np.reshape(np.array(node['sigma']),(dim,dim))
        mean = np.array(node['mean'])
        conseq = np.array(node['consequence'])
        return ComplexRule(conseq[0:-1],conseq[-1],mean,sigma)
