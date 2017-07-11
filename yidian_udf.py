#!/usr/bin/python
#coding=utf8

import types
import math
import sys

reload(sys)
sys.setdefaultencoding('UTF-8')

def outputSchema(schema_def):
    def decorator(func):
        func.outputSchema = schema_def
        return func
    return decorator

def outputSchemaFunction(schema_def):
    def decorator(func):
        func.outputSchemaFunction = schema_def
        return func
    return decorator

def schemaFunction(schema_def):
    def decorator(func):
        func.schemaFunction = schema_def
        return func
    return decorator

# Usage: yidian_udf.ParseEventFeature(line)
@outputSchema("t:(timestamp:int, userid:chararray, docid:chararray, label:int, feature:chararray)")
def ParseEventFeature(line, sep):
    fields = line.strip().split(sep)
    try:
        timestamp = int(fields[0])
        userid = fields[1]
        docid = fields[2]
        label = int(fields[3])
        feature = sep.join(fields[4:])
        return (timestamp, userid, docid, label, feature);
    except:
        return None

# Usage: yidian_udf.ParseEventFeatureWithWeight(line)
@outputSchema("t:(timestamp:int, userid:chararray, docid:chararray, label:int, weight:double, feature:chararray)")
def ParseEventFeatureWithWeight(line, sep):
    fields = line.strip().split(sep)
    try:
        timestamp = int(fields[0])
        userid = fields[1]
        docid = fields[2]
        label = int(fields[3])
        weight = float(fields[4])
        feature = sep.join(fields[5:])
        return (timestamp, userid, docid, label, weight, feature);
    except:
        return None

# Usage: yidian_udf.ExtractSchema(schema_file, sep, line, fea1, fea2, ...)
@outputSchema("out:()")
def ExtractSchema(schema_file, sep, feature, *args):
    if ExtractSchema.schema is None:
        ExtractSchema.schema = dict()
        for ln in open(schema_file):
            fields = ln.strip('\n').split(sep)
            try:
                # map schema name to position
                ExtractSchema.schema[fields[1]] = int(fields[0])
            except:
                continue
    fields = feature.strip('\n').split(sep)
    out = []
    for a in args:
        p = ExtractSchema.schema.get(a)
        if p <> None:
            out.append(fields[p])
    if len(out) <> len(args):
        return None
    else:
        return tuple(out)
ExtractSchema.schema = None

def nr(y, a, b, debug = 0):
    x = a
    max_iter = 50
    iter = 0
    
    while(iter < max_iter):
        e = math.exp(y * x)
        f = x - a - b * y / (1.0 + e)
        if math.fabs(f) < 1.e-5: break
        ff = 1 + b * e / ((1.0 + e) * (1.0 + e))
        x = x - f / ff
        iter += 1
        if debug: print >> sys.stderr, iter, f, x

    return x
# Usage: yidian_udf.SimpleUserOlr(D.(timestamp, label, weight, pos_bias, item_bias, item_var, *features), P.(interest, mean, var, obs, pos), default_var)
@outputSchema("posterior:{t:(interest:chararray, mean:double, var:double, obs:double, pos:double)}")
def SimpleUserOlr(events, prior, default_var):
    if events == None or len(events) == 0: return prior;
    mdl = dict()
    for interest,mean,var,obs,pos in prior:
        mdl[interest] = [mean, var, obs, pos]
    for sample in events:
        (timestamp, label, weight, pos_bias, item_bias, var_info) = sample[:6]
        label = 2 * label - 1
        a = item_bias + pos_bias
        b = 0.0
        try:
            b += float(var_info)
        except:
            found = False
            for s in var_info.split(' '):
                fields = s.split(':')
                if fields[0] == 'docid' and len(fields) > 3:
                    b += float(fields[2])
                    found = True
                    break
            if not found: raise
        for upid in sample[6:]:
            if mdl.get(upid) is None:
                mdl[upid] = [0.0, default_var, 0, 0]
            a += mdl[upid][0]
            b += mdl[upid][1]

        # for dislike events ignore position bias
        if weight > 1.0 and label < 0:
            a -= pos_bias

        x = nr(label, a, b)

        e = math.exp(label * x)

        for upid in sample[6:]:
            if mdl[upid][1] > 0:
                mdl[upid][0] += weight * mdl[upid][1] * label / (1.0 + e)
                mdl[upid][1] = 1.0 / (1.0 / mdl[upid][1] + weight * e / ((1.0 + e) * (1.0 + e)))
                mdl[upid][2] += 1
                mdl[upid][3] += ((label + 1) / 2)

    posterior = []
    for k,v in mdl.items():
        l = [k]
        l.extend(v)
        posterior.append(tuple(l))

    return posterior

# Usage: yidian_udf.AttachBatchId(inbag:{t:(timestamp:long)})
@outputSchema("outbag:{t:(timestamp:long, batchid:long)}")
def AttachBatchId(inbag):
    outbag = []
    for i,k in enumerate(inbag):
        outbag.append((k[0], i))
    return outbag

# Usage: yidian_udf.AttachSessionBatchId(inbag:{t:(timestamp:long)}, session_in_seconds:long)
@outputSchema("outbag:{t:(timestamp:long, batchid:long)}")
def AttachSessionBatchId(inbag, session_in_seconds):
    outbag = []
    batch = 0
    action = inbag[0][0]
    outbag.append((action, batch))
    lastaction = action
    for k in range(1, len(inbag)):
        action = inbag[k][0]
        if (action - lastaction) < session_in_seconds:
            batch += 1
        else:
            batch = 0
        outbag.append((action, batch))
        lastaction = action
    return outbag

# Usage: yidian_udf.ListToBag(f1:chararray, f2:chararray, ...)
@outputSchema("outbag:{t:(f:chararray)}")
def ListToBag(*args):
    outbag = []
    for a in args:
        outbag.append((a,))
    return outbag

# Usage: yidian_udf.Strcat(...)
@outputSchema("out:chararray")
def Strcat(*args):
    return ''.join(args)

# Usage: yidian_udf.BagToStr(B:{t:()}, tsep, fsep)
@outputSchema("out:chararray")
def BagToStr(B, tsep, fsep):
    return tsep.join([fsep.join(map(str, t)) for t in B])

@outputSchema("out:boolean")
def JudgeCategory(interest,interests):
    interests = interests.split(',')
    interests = filter(lambda x:len(x)>0,interests)
    if len(interests) == 0:
        return True
    return interest in interests

if __name__ == '__main__':
    import sys
    for ln in open(sys.argv[1]):
        try:
            (timestamp, userid, docid, label, weight, feature) = ParseEventFeatureWithWeight(ln, '\t')
            (ct, sct) = ExtractSchema(sys.argv[2], '\t', feature, 'ct', 'sct')
            print timestamp, userid, docid, label, weight, ct, sct
        except:
            continue

