from __future__ import division


def mean(rows, columns):
    
    cols = columns+["time"]
    
    row = {}
    for col in cols:
        row[col] = 0
    row["time"] = None
    
    length = len(rows)
    
    for r in rows:
        for col in cols:
            if col == "time" and row[col] == None:
                #print "time", r[col]
                row[col] = r[col]
            else:
                if col != "time":
                    row[col] += r[col]
             
    for r in row.keys():
        if r != "time":
            row[r] = row[r] / length
        else:
            row[r] = "'"+row[r]+"'"
        
    
    return row
